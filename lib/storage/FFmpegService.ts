// FFmpegService.ts
import { spawn } from 'child_process';
import * as fs from 'fs/promises';
import * as path from 'path';
import { randomUUID } from 'crypto'; // 用于生成唯一目录名
import * as os from 'os';

// --- FFmpeg 模块配置接口 ---
export interface RenditionConfig {
  label: string; // 例如 "1080p", "720p"。用于文件名和S3路径。
  width: number;
  height: number;
  videoBitrate: string; // 例如 "5000k"
  audioBitrate: string; // 例如 "128k"
  crf?: number; // 恒定速率因子，例如 20
  preset?: string; // FFmpeg预设，例如 "medium", "fast", "slower"
  profile?: string; // H.264 Profile, 例如 "main", "high"
  level?: string; // H.264 Level, 例如 "4.0"
}

export interface FFmpegHLSProcessingOptions {
  renditions?: RenditionConfig[]; // 如果未提供，则使用默认码率配置
  hlsTime?: number; // HLS片段时长（秒）
  ffmpegPath?: string; // FFmpeg可执行文件路径，默认为 'ffmpeg'
  outputDirName?: string; // 在基础临时目录中为特定作业指定的输出子目录名。默认为videoId或UUID。
  baseTempDir?: string; // 存放HLS文件的基础临时目录。默认为 'os.tmpdir()/ffmpeg_hls_temp'。
  s3KeyPrefix?: string; // S3存储键的前缀，例如 "hls" 或 "videos/processed"
}

export interface HLSFile {
  localPath: string; // 本地文件的完整路径
  s3Key: string;     // 目标S3存储键
  contentType: string; // 文件MIME类型
}

export interface HLSProcessingResult {
  localOutputDir: string;         // HLS文件在本地生成的目录
  masterPlaylistS3Key: string;  // 主播放列表的完整S3存储键
  filesToUpload: HLSFile[];     // 需要上传的文件列表
}

// 默认码率配置
const DEFAULT_RENDITIONS: RenditionConfig[] = [
  { label: '1080p', width: 1920, height: 1080, videoBitrate: '5000k', audioBitrate: '192k', crf: 20, preset: 'medium', profile: 'high', level: '4.2' },
  { label: '720p', width: 1280, height: 720, videoBitrate: '2800k', audioBitrate: '128k', crf: 22, preset: 'medium', profile: 'main', level: '3.1' },
  { label: '480p', width: 854, height: 480, videoBitrate: '1400k', audioBitrate: '96k', crf: 23, preset: 'medium', profile: 'main', level: '3.1' },
];

export class FFmpegService {
  private ffmpegPath: string;

  constructor(ffmpegPath: string = 'ffmpeg') {
    this.ffmpegPath = ffmpegPath;
    // 可以在这里添加检查FFmpeg是否可访问的逻辑
  }

  /**
   * 将本地视频文件处理为HLS格式。
   * @param localVideoPath 本地视频文件的路径。
   * @param videoId 用于S3 Key结构和本地临时文件夹名称的唯一ID。
   * @param options 处理选项。
   * @returns 包含处理结果的对象。
   */
  async processVideoToHLS(
    localVideoPath: string,
    videoId: string,
    options: FFmpegHLSProcessingOptions = {}
  ): Promise<HLSProcessingResult> {
    const renditions = options.renditions || DEFAULT_RENDITIONS;
    const hlsTime = options.hlsTime || 4; // HLS片段时长，例如4秒
    const s3KeyPrefix = options.s3KeyPrefix || 'hls'; // S3存储路径前缀
    const baseTempDir = options.baseTempDir || path.join(os.tmpdir(), 'ffmpeg_hls_processing');
    const jobSpecificDirName = options.outputDirName || videoId; // 使用videoId作为子目录名
    const outputDir = path.join(baseTempDir, jobSpecificDirName);

    try {
      await fs.mkdir(outputDir, { recursive: true });
      console.log(`Created temporary HLS output directory: ${outputDir}`);

      const masterPlaylistName = 'master.m3u8';
      const args: string[] = ['-hide_banner', '-y', '-i', localVideoPath];

      // 构建 filter_complex 参数用于视频缩放
      let filterComplex = '';
      renditions.forEach((rendition, index) => {
        // [0:v] 表示第一个输入文件的视频流
        filterComplex += `[0:v]scale=w=${rendition.width}:h=${rendition.height}:force_original_aspect_ratio=decrease:force_divisible_by=2,format=yuv420p[v${index}];`;
      });
      args.push('-filter_complex', filterComplex.trim().slice(0, -1)); // 移除末尾的分号

      // 为每个码率版本构建输出流映射和编解码选项
      let varStreamMap = '';
      renditions.forEach((rendition, index) => {
        args.push(`-map`, `[v${index}]`); // 映射处理后的视频流
        args.push(`-c:v:${index}`, 'libx264');
        if (rendition.profile) args.push(`-profile:v:${index}`, rendition.profile);
        if (rendition.level) args.push(`-level:v:${index}`, rendition.level);
        args.push(`-preset:v:${index}`, rendition.preset || 'medium');
        if (rendition.crf) args.push(`-crf:${index}`, rendition.crf.toString());
        args.push(`-b:v:${index}`, rendition.videoBitrate);
        args.push(`-maxrate:${index}`, rendition.videoBitrate); // 可以根据需要调整
        args.push(`-bufsize:${index}`, `calc(${rendition.videoBitrate} * 2)`); // 估算bufsize
        args.push(`-g:${index}`, (hlsTime * 25 * 2).toString()); // GOP 大小，假设帧率为25fps，2秒一个I帧
        args.push(`-keyint_min:${index}`, (hlsTime * 25 * 2).toString());
        args.push(`-sc_threshold:${index}`, '0');


        args.push(`-map`, `0:a:0?`); // 映射第一个音频流，如果存在 ('?' 使其可选)
        args.push(`-c:a:${index}`, 'aac');
        args.push(`-b:a:${index}`, rendition.audioBitrate);
        args.push(`-ar:${index}`, '48000'); // 音频采样率
        args.push(`-ac:${index}`, '2');    // 立体声

        // 构建 var_stream_map 参数
        varStreamMap += `v:${index},a:${index},name:${rendition.label} `;
      });

      args.push('-f', 'hls');
      args.push('-hls_time', hlsTime.toString());
      args.push('-hls_playlist_type', 'vod'); // 'vod' 表示视频点播
      args.push('-hls_flags', 'independent_segments');
      // %v 会被 -var_stream_map 中的 'name' (即rendition.label) 替换
      args.push('-hls_segment_filename', path.join(outputDir, `${videoId}_%v_segment_%03d.ts`));
      args.push('-master_pl_name', masterPlaylistName); // 主播放列表文件名
      args.push('-var_stream_map', varStreamMap.trim());
      // %v 同样会被 'name' 替换，生成如 1080p.m3u8, 720p.m3u8 的变体播放列表
      args.push(path.join(outputDir, `${videoId}_%v.m3u8`));

      console.log(`Executing FFmpeg command:`);
      console.log(`${this.ffmpegPath} ${args.map(arg => arg.includes(" ") ? `"${arg}"` : arg).join(' ')}`);


      await new Promise<void>((resolve, reject) => {
        const ffmpegProcess = spawn(this.ffmpegPath, args, { stdio: 'pipe' });

        let stderrOutput = '';
        ffmpegProcess.stdout?.on('data', (data) => console.log(`FFMPEG_STDOUT: ${data.toString().trim()}`));
        ffmpegProcess.stderr?.on('data', (data) => {
          const line = data.toString().trim();
          console.error(`FFMPEG_STDERR: ${line}`);
          stderrOutput += line + '\n';
        });

        ffmpegProcess.on('close', (code) => {
          if (code === 0) {
            console.log('FFmpeg processing completed successfully.');
            resolve();
          } else {
            console.error(`FFmpeg process exited with code ${code}`);
            reject(new Error(`FFmpeg process exited with code ${code}. FFmpeg output:\n${stderrOutput}`));
          }
        });
        ffmpegProcess.on('error', (err) => {
          console.error('Failed to start FFmpeg process:', err);
          reject(err);
        });
      });

      // 收集生成的文件信息
      const filesInOutputDir = await fs.readdir(outputDir);
      const filesToUpload: HLSFile[] = [];
      const s3ObjectPrefix = `${s3KeyPrefix}/${videoId}`; // 例如: hls/your-video-id

      for (const file of filesInOutputDir) {
        const localPath = path.join(outputDir, file);
        const s3Key = `${s3ObjectPrefix}/${file}`; // S3中的相对路径
        let contentType = 'application/octet-stream'; //默认
        if (file.endsWith('.m3u8')) {
          contentType = 'application/vnd.apple.mpegurl'; // 或者 'application/x-mpegURL'
        } else if (file.endsWith('.ts')) {
          contentType = 'video/MP2T';
        }
        filesToUpload.push({ localPath, s3Key, contentType });
      }

      const masterPlaylistS3Key = `${s3ObjectPrefix}/${masterPlaylistName}`;

      return {
        localOutputDir: outputDir,
        masterPlaylistS3Key,
        filesToUpload,
      };
    } catch (error) {
      console.error('Error during HLS processing:', error);
      // 发生错误时，尝试清理临时目录（如果已创建）
      if (outputDir) {
        try {
          await fs.rm(outputDir, { recursive: true, force: true });
          console.log(`Cleaned up temporary directory on error: ${outputDir}`);
        } catch (cleanupError) {
          console.error(`Failed to cleanup temporary directory on error: ${outputDir}`, cleanupError);
        }
      }
      throw error; // 重新抛出错误
    }
  }

  /**
   * 清理指定的本地目录。
   * @param directoryPath 要清理的目录路径。
   */
  async cleanupDirectory(directoryPath: string): Promise<void> {
    try {
      console.log(`Attempting to clean up directory: ${directoryPath}`);
      await fs.rm(directoryPath, { recursive: true, force: true });
      console.log(`Successfully cleaned up directory: ${directoryPath}`);
    } catch (error) {
      console.error(`Failed to clean up directory ${directoryPath}:`, error);
      // 可以选择是否抛出错误，或者只是记录它
    }
  }
}