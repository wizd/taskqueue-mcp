// VideoPipeline.ts
import * as fsNode from 'fs'; // Node.js fs for createReadStream
import { FFmpegService, HLSProcessingResult, FFmpegHLSProcessingOptions } from './FFmpegService'; // 假设在同一目录下
import { S3VideoHostingService, VideoHostingConfig } from './S3VideoHostingService'; // 假设在同一目录下
import { randomUUID } from 'crypto';

// --- 主流程函数 ---
export async function runFullVideoPipeline(
  localVideoPath: string,
  s3Config: VideoHostingConfig,
  ffmpegProcessingOptions?: FFmpegHLSProcessingOptions,
  customVideoId?: string, // 允许传入自定义的 videoId
  cleanupLocalFiles: boolean = true // 是否在上传成功后清理本地HLS文件
): Promise<string> {
  const videoId = customVideoId || `video-${randomUUID()}`; // 为该视频生成唯一ID

  const ffmpegService = new FFmpegService(ffmpegProcessingOptions?.ffmpegPath); // 使用选项中的ffmpeg路径或默认值
  const s3Service = new S3VideoHostingService(s3Config);

  let processingResult: HLSProcessingResult | null = null;

  try {
    console.log(`[Pipeline] Starting processing for video: ${localVideoPath} with ID: ${videoId}`);
    processingResult = await ffmpegService.processVideoToHLS(
      localVideoPath,
      videoId,
      ffmpegProcessingOptions
    );
    console.log(`[Pipeline] Video processing complete. HLS files generated in: ${processingResult.localOutputDir}`);

    console.log(`[Pipeline] Uploading ${processingResult.filesToUpload.length} HLS files to S3...`);

    // 并行上传所有文件
    const uploadPromises = processingResult.filesToUpload.map(fileToUpload => {
      const fileStream = fsNode.createReadStream(fileToUpload.localPath);
      return s3Service.uploadFile({
        fileStream: fileStream as any,
        key: fileToUpload.s3Key,
        contentType: fileToUpload.contentType,
        acl: 'public-read', // HLS 文件通常需要公开可读
      }).then(details => {
        console.log(`[Pipeline] Uploaded ${fileToUpload.s3Key} to ${details.url}`);
        return details;
      }).catch(uploadError => {
        console.error(`[Pipeline] Failed to upload ${fileToUpload.s3Key}:`, uploadError);
        throw uploadError; // 让 Promise.all 捕获
      });
    });

    await Promise.all(uploadPromises);
    console.log('[Pipeline] All HLS files uploaded to S3 successfully.');

    // 获取主播放列表的最终可访问URL
    // 假设S3文件是公开的或配置了publicBaseUrl，所以isPublic=true
    const finalMasterPlaylistUrl = await s3Service.getFileUrl(
      processingResult.masterPlaylistS3Key,
      3600, // 预签名URL有效期（如果需要的话）
      true  // 假设存储桶/文件是公开的，或者设置了 publicBaseUrl
    );

    console.log(`[Pipeline] SUCCESS! Playable HLS URL: ${finalMasterPlaylistUrl}`);
    return finalMasterPlaylistUrl;

  } catch (error) {
    console.error('[Pipeline] Video processing and uploading pipeline FAILED:', error);
    throw error; // 重新抛出，以便上层调用者处理
  } finally {
    // 清理本地生成的HLS文件
    if (processingResult?.localOutputDir && cleanupLocalFiles) {
      console.log(`[Pipeline] Cleaning up local HLS files from: ${processingResult.localOutputDir}`);
      await ffmpegService.cleanupDirectory(processingResult.localOutputDir);
    } else if (processingResult?.localOutputDir && !cleanupLocalFiles) {
      console.log(`[Pipeline] Local HLS files preserved at: ${processingResult.localOutputDir}`);
    }
  }
}

// --- 使用示例 ---
// (取消注释并替换为您的实际配置和路径来运行)
export async function HostVideoToR2(localVideoPath: string, tenantId: string, s3Config?: VideoHostingConfig, ffmpegProcessingOptions?: FFmpegHLSProcessingOptions, customVideoId?: string, cleanupLocalFiles: boolean = true) {
  if (!process.env.R2_ENDPOINT_URL || !process.env.R2_ACCESS_KEY_ID || !process.env.R2_SECRET_ACCESS_KEY || !process.env.R2_VIDEO_BUCKET_NAME || !process.env.R2_PUBLIC_BUCKET_URL) {
    console.error('R2 environment variables are not set. Please set them in your environment.');
    throw new Error('R2 environment variables are not set. Please set them in your environment.');
  }
  const exampleS3Config: VideoHostingConfig = {
    region: 'auto', // Cloudflare R2 特定
    endpoint: process.env.R2_ENDPOINT_URL, // 例如: 'https://<ACCOUNT_ID>.r2.cloudflarestorage.com'
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    },
    bucket: process.env.R2_VIDEO_BUCKET_NAME,
    publicBaseUrl: process.env.R2_PUBLIC_BUCKET_URL, // 例如: 'https://pub-<YOUR_R2_PUBLIC_BUCKET_ID>.r2.dev'
  };

  const localVideo = localVideoPath;

  if (!fsNode.existsSync(localVideo)) {
    console.error(`Error: Video file not found at ${localVideo}`);
    console.log("Please replace './path/to/your/sample-video.mp4' with an actual video file path.");
    return;
  }

  try {
    const playableUrl = await runFullVideoPipeline(
      localVideo,      
      exampleS3Config,
      { // 可选的 FFmpeg 处理选项
        // renditions: [ // 自定义码率
        //   { label: '720p_custom', width: 1280, height: 720, videoBitrate: '2000k', audioBitrate: '128k' }
        // ],
        hlsTime: 6, // 6秒一个片段
        s3KeyPrefix: tenantId + '_videos/hls' // 自定义S3路径前缀
      },
      randomUUID(),
      // `custom-video-id-${Date.now()}`, // 可选的自定义 videoId
      true // 在成功上传后清理本地HLS文件
    );
    console.log(`\n========================================`);
    console.log(`✅ Final Playable URL: ${playableUrl}`);
    console.log(`========================================`);
  } catch (error) {
    console.error(`\n🔴 Pipeline execution failed overall:`, error);
  }
}
/*
main();
*/