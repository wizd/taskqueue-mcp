// 安装 AWS SDK v3 (如果尚未安装):
// npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner
// yarn add @aws-sdk/client-s3 @aws-sdk/s3-request-presigner

import {
    S3Client,
    PutObjectCommand,
    DeleteObjectCommand,
    ListObjectsV2Command,
    GetObjectCommand,
    S3ClientConfig,
  } from '@aws-sdk/client-s3';
  import { getSignedUrl } from '@aws-sdk/s3-request-presigner'; // 用于生成预签名URL以下载私有内容
  
  // --- 配置接口 ---
  export interface VideoHostingConfig {
    region: string; // 例如: 'auto' 对于 Cloudflare R2
    endpoint: string; // 例如: 'https<ACCOUNT_ID>.r2.cloudflarestorage.com'
    credentials: {
      accessKeyId: string;
      secretAccessKey: string;
    };
    bucket: string;
    publicBaseUrl?: string; // 可选，用于构建公共可访问的URL，例如 'https://pub-<YOUR_R2_PUBLIC_BUCKET_ID>.r2.dev'
  }
  
  // --- 视频元数据接口 ---
  export interface VideoDetails {
    key: string; // 在存储桶中的完整路径/键
    url: string; // 公共访问URL或预签名URL
    eTag?: string; // S3返回的ETag
    size?: number;
    lastModified?: Date;
  }
  
  export interface UploadFileParams {
    fileStream: ReadableStream | Blob | Buffer | string; // 文件内容
    key: string; // 在存储桶中期望的完整路径，例如 'videos/my-event/video.mp4' 或 'hls/my-stream/playlist.m3u8'
    contentType: string; // 例如 'video/mp4', 'application/x-mpegURL', 'video/MP2T'
    acl?: 'private' | 'public-read'; // 访问控制，Cloudflare R2 默认为 private
  }
  
  export class S3VideoHostingService {
    private s3Client: S3Client;
    private config: VideoHostingConfig;
  
    constructor(config: VideoHostingConfig) {
      this.config = config;
      const s3Config: S3ClientConfig = {
        region: this.config.region,
        endpoint: this.config.endpoint,
        credentials: this.config.credentials,
        forcePathStyle: true, // 对于某些S3兼容服务可能是必需的
      };
      this.s3Client = new S3Client(s3Config);
    }
  
    /**
     * 上传单个文件到 S3 兼容存储。
     * 这可以用于上传视频文件、HLS播放列表文件(m3u8)或HLS片段文件(ts)。
     * @param params 上传参数
     * @returns 包含文件key和URL的视频详情
     */
    async uploadFile(params: UploadFileParams): Promise<VideoDetails> {
      const command = new PutObjectCommand({
        Bucket: this.config.bucket,
        Key: params.key,
        Body: params.fileStream,
        ContentType: params.contentType,
        ACL: params.acl || 'private', // 根据需要设置ACL
      });
  
      try {
        const response = await this.s3Client.send(command);
        const url = this.config.publicBaseUrl
          ? `${this.config.publicBaseUrl}/${params.key}`
          : `s3://${this.config.bucket}/${params.key}`; // 如果没有公共URL基础，则返回S3 URI
  
        return {
          key: params.key,
          url: url, // 注意：如果对象是私有的，这个URL可能无法直接访问，需要预签名URL
          eTag: response.ETag,
        };
      } catch (error) {
        console.error(`Error uploading file ${params.key}:`, error);
        throw error;
      }
    }
  
    /**
     * 获取文件的公共URL或预签名URL（如果文件是私有的）。
     * @param key 文件在存储桶中的键
     * @param expiresInSeconds 预签名URL的有效期（秒），默认为3600 (1小时)
     * @param isPublic 是否文件本身就是公开可读的
     * @returns 文件的访问URL
     */
    async getFileUrl(key: string, expiresInSeconds: number = 3600, isPublic: boolean = false): Promise<string> {
      if (isPublic && this.config.publicBaseUrl) {
        return `${this.config.publicBaseUrl}/${key}`;
      } else if (this.config.publicBaseUrl && !isPublic) {
          console.warn(`File with key ${key} is not public, but a publicBaseUrl is configured. Consider using a presigned URL or making the object public.`);
      }
  
      // 对于私有对象，或者没有配置 publicBaseUrl 的情况，生成预签名URL
      const command = new GetObjectCommand({
        Bucket: this.config.bucket,
        Key: key,
      });
      try {
        const signedUrl = await getSignedUrl(this.s3Client, command, { expiresIn: expiresInSeconds });
        return signedUrl;
      } catch (error) {
        console.error(`Error generating signed URL for ${key}:`, error);
        throw error;
      }
    }
  
    /**
     * 删除存储桶中的文件。
     * @param key 要删除的文件键
     */
    async deleteFile(key: string): Promise<void> {
      const command = new DeleteObjectCommand({
        Bucket: this.config.bucket,
        Key: key,
      });
      try {
        await this.s3Client.send(command);
        console.log(`File ${key} deleted successfully.`);
      } catch (error) {
        console.error(`Error deleting file ${key}:`, error);
        throw error;
      }
    }
  
    /**
     * 列出存储桶中指定前缀下的对象。
     * @param prefix 用于过滤对象的前缀 (例如 'videos/' 或 'hls/my-stream/')
     * @returns 对象列表
     */
    async listFiles(prefix?: string): Promise<VideoDetails[]> {
      const command = new ListObjectsV2Command({
        Bucket: this.config.bucket,
        Prefix: prefix,
      });
  
      try {
        const response = await this.s3Client.send(command);
        const files: VideoDetails[] = response.Contents?.map(item => {
          const key = item.Key!;
          const url = this.config.publicBaseUrl
            ? `${this.config.publicBaseUrl}/${key}`
            : `s3://${this.config.bucket}/${key}`;
          return {
            key: key,
            url: url, // 同样，这可能是S3 URI或需要预签名的公共URL
            size: item.Size,
            lastModified: item.LastModified,
            eTag: item.ETag
          };
        }) || [];
        return files;
      } catch (error) {
        console.error(`Error listing files with prefix ${prefix}:`, error);
        throw error;
      }
    }
  
    /**
     * 辅助函数：上传 HLS 视频流的所有文件。
     * 假设你有一个包含 master.m3u8 和所有 .ts 片段的目录结构。
     * 注意：此函数为概念性演示，实际文件读取和流处理取决于运行环境（Node.js vs 浏览器）。
     * 在浏览器中，通常会逐个上传 File 对象。在 Node.js 中，可以读取文件系统。
     *
     * @param files 文件对象的数组，每个对象包含本地路径、目标 S3 key 和内容类型。
     * 例如: [{ fileContent: ReadableStream, key: 'hls/stream1/master.m3u8', contentType: 'application/x-mpegURL' },
     * { fileContent: ReadableStream, key: 'hls/stream1/segment0.ts', contentType: 'video/MP2T' }, ...]
     * @param acl 访问控制
     * @returns 返回主播放列表 (master.m3u8) 的详情
     */
    async uploadHLSStream(
      files: Array<{ fileStream: ReadableStream | Blob | Buffer | string; key: string; contentType: string }>,
      acl: 'private' | 'public-read' = 'public-read' // HLS 文件通常需要公开读取
    ): Promise<VideoDetails> {
      if (files.length === 0) {
        throw new Error('No files provided for HLS stream upload.');
      }
  
      let masterPlaylistDetails: VideoDetails | null = null;
  
      for (const file of files) {
        const details = await this.uploadFile({
          fileStream: file.fileStream,
          key: file.key,
          contentType: file.contentType,
          acl: acl,
        });
        if (file.key.endsWith('.m3u8') && !file.key.includes('variant')) { // 简单判断是否为主播放列表
            masterPlaylistDetails = details;
        }
        console.log(`Uploaded HLS file: ${file.key} to ${details.url}`);
      }
  
      if (!masterPlaylistDetails) {
          // 尝试查找任何m3u8文件作为备选
          const anyM3U8 = files.find(f => f.key.endsWith('.m3u8'));
          if (anyM3U8) {
              const url = this.config.publicBaseUrl
                  ? `${this.config.publicBaseUrl}/${anyM3U8.key}`
                  : `s3://${this.config.bucket}/${anyM3U8.key}`;
              masterPlaylistDetails = { key: anyM3U8.key, url };
              console.warn("Could not determine a definitive master playlist. Using the first m3u8 found.");
          } else {
              throw new Error('Master playlist (.m3u8) not found or uploaded in HLS stream.');
          }
      }
      // 更新主播放列表的 URL 以确保其准确性，特别是如果需要预签名
      masterPlaylistDetails.url = await this.getFileUrl(masterPlaylistDetails.key, 3600, acl === 'public-read');
  
      return masterPlaylistDetails;
    }
  }
  
  // --- 使用示例 (概念性) ---
  async function exampleUsage() {
    // 1. 配置 (从环境变量、配置文件等加载)
    const hostingConfig: VideoHostingConfig = {
      region: 'auto', // Cloudflare R2 特定
      endpoint: 'https_YOUR_ACCOUNT_ID.r2.cloudflarestorage.com',
      credentials: {
        accessKeyId: 'YOUR_R2_ACCESS_KEY_ID',
        secretAccessKey: 'YOUR_R2_SECRET_ACCESS_KEY',
      },
      bucket: 'your-video-bucket-name',
      publicBaseUrl: 'https_pub-YOUR_R2_PUBLIC_BUCKET_ID.r2.dev', // 如果你的 R2 存储桶已启用公共访问
    };
  
    const videoService = new S3VideoHostingService(hostingConfig);
  
    // --- 场景 A: 上传单个 MP4 文件 ---
    // 在 Node.js 中:
    // import * as fs from 'fs';
    // const videoFilePath = './path/to/your/video.mp4';
    // const fileStream = fs.createReadStream(videoFilePath);
    // const stats = fs.statSync(videoFilePath);
  
    // 在浏览器中 (fileInput 是 <input type="file"> 元素):
    // const fileInput = document.getElementById('fileInput') as HTMLInputElement;
    // const file = fileInput.files?.[0];
    // if (!file) { console.error("No file selected"); return; }
    // const fileStream = file.stream(); // 或直接用 file 对象作为 Body
  
    /*
    // 假设这是 Node.js 环境
    const videoKey = `videos/my-cool-screencast-${Date.now()}.mp4`;
    try {
      // 假设 fileStream 和 contentType 已定义
      // const mp4Details = await videoService.uploadFile({
      //   fileStream: fileStream, // 来自 fs.createReadStream(filePath) 或浏览器 File API
      //   key: videoKey,
      //   contentType: 'video/mp4',
      //   acl: 'public-read',
      // });
      // console.log('MP4 Video uploaded:', mp4Details);
      // const accessibleUrl = await videoService.getFileUrl(mp4Details.key, 3600, true);
      // console.log('Accessible URL:', accessibleUrl);
  
    } catch (err) {
      console.error('Failed to upload MP4:', err);
    }
    */
  
    // --- 场景 B: 上传 HLS 文件集 ---
    // 假设你已经用 FFmpeg 将视频转换为 HLS，并得到如下文件结构:
    // - hls/
    //   - my-stream/
    //     - master.m3u8
    //     - variant_stream_1.m3u8
    //     - segment_1_0.ts
    //     - segment_1_1.ts
    //     - ...
    //     - variant_stream_2.m3u8
    //     - segment_2_0.ts
    //     - segment_2_1.ts
    //     - ...
  
    // 你需要准备一个文件列表来上传
    // 在 Node.js 中，你可以使用 fs.readdirSync 递归读取目录并创建流
    const hlsFilesToUpload = [
      // { fileStream: fs.createReadStream('./local-hls-path/master.m3u8'), key: 'hls/my-stream/master.m3u8', contentType: 'application/x-mpegURL' },
      // { fileStream: fs.createReadStream('./local-hls-path/variant_1.m3u8'), key: 'hls/my-stream/variant_1.m3u8', contentType: 'application/x-mpegURL' },
      // { fileStream: fs.createReadStream('./local-hls-path/segment_1_0.ts'), key: 'hls/my-stream/segment_1_0.ts', contentType: 'video/MP2T' },
      // ... more segments and variant playlists
    ];
  
    /*
    if (hlsFilesToUpload.length > 0) {
        try {
          const hlsMasterPlaylistDetails = await videoService.uploadHLSStream(hlsFilesToUpload, 'public-read');
          console.log('HLS Stream master playlist uploaded:', hlsMasterPlaylistDetails);
          // URL 已经是公开的了，因为ACL是 'public-read' 且配置了 publicBaseUrl
          console.log('HLS Master Playlist URL:', hlsMasterPlaylistDetails.url);
        } catch (err) {
          console.error('Failed to upload HLS stream:', err);
        }
    }
    */
  
  
    // --- 场景 C: 列出视频 ---
    try {
      const allVideos = await videoService.listFiles('videos/');
      console.log('All videos in "videos/" path:', allVideos);
  
      const specificHLSStreamFiles = await videoService.listFiles('hls/my-stream/');
      console.log('Files for HLS stream "my-stream":', specificHLSStreamFiles);
    } catch (err) {
      console.error('Failed to list files:', err);
    }
  
    // --- 场景 D: 删除视频 (例如，删除之前上传的单个 MP4) ---
    /*
    try {
      // await videoService.deleteFile(videoKey); // 使用之前上传的 videoKey
      // console.log(`File ${videoKey} should now be deleted.`);
    } catch (err) {
      console.error('Failed to delete file:', err);
    }
    */
  }
  
  // 调用示例 (确保在 async 上下文中运行或使用 .then().catch())
  // exampleUsage().catch(console.error);