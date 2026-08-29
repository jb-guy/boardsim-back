import {
  Controller,
  Param,
  Post,
  UploadedFile,
  UseInterceptors,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { diskStorage } from 'multer';
import * as fs from 'fs';
import * as path from 'path';
import { LobbyManager } from '../lobby/lobby-manager.js';
import { LobbyGateway } from '../lobby/lobby.gateway.js';
import type { Asset, AssetType } from '../../types/index.js';
import { randomUUID } from 'crypto';

function mimeToAssetType(mimeType: string): AssetType {
  if (mimeType.startsWith('image/')) return 'image';
  if (mimeType === 'application/pdf') return 'pdf';
  return 'image'; // default fallback for Phase 1
}

function extFromMime(mimeType: string): string {
  const map: Record<string, string> = {
    'image/jpeg': '.jpg',
    'image/jpg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/svg+xml': '.svg',
    'application/pdf': '.pdf',
  };
  return map[mimeType] ?? path.extname('file');
}

@Controller('upload')
export class UploadController {
  constructor(
    private readonly lobbyManager: LobbyManager,
    private readonly lobbyGateway: LobbyGateway,
  ) {}

  @Post(':token')
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: (_req, _file, cb) => {
          // Destination is resolved per-request after token validation.
          // We use a temporary directory and move the file after token consumption.
          const tmpDir = path.join(process.cwd(), 'uploads', '_tmp');
          fs.mkdirSync(tmpDir, { recursive: true });
          cb(null, tmpDir);
        },
        filename: (_req, _file, cb) => {
          cb(null, randomUUID());
        },
      }),
      limits: {
        fileSize: 50 * 1024 * 1024, // 50 MB
      },
    }),
  )
  async handleUpload(
    @Param('token') token: string,
    @UploadedFile() file: Express.Multer.File,
  ): Promise<{ success: true; assetId: string }> {
    if (!file) {
      throw new HttpException('No file provided', HttpStatus.BAD_REQUEST);
    }

    const pending = this.lobbyManager.consumeUploadToken(token);
    if (!pending) {
      // Clean up the temp file
      fs.unlink(file.path, () => undefined);
      throw new HttpException(
        'Invalid or expired upload token',
        HttpStatus.UNPROCESSABLE_ENTITY,
      );
    }

    const ext = extFromMime(pending.mimeType);
    const destDir = path.join(process.cwd(), 'uploads', pending.lobbyId);
    fs.mkdirSync(destDir, { recursive: true });

    const destFile = path.join(destDir, `${pending.assetId}${ext}`);

    try {
      fs.renameSync(file.path, destFile);
    } catch {
      // renameSync can fail across filesystems; fall back to copy + delete
      fs.copyFileSync(file.path, destFile);
      fs.unlinkSync(file.path);
    }

    const serverBaseUrl =
      process.env.NESTJS_BASE_URL ?? 'http://localhost:3001';
    const source = `${serverBaseUrl}/uploads/${pending.lobbyId}/${pending.assetId}${ext}`;

    const asset: Asset = {
      id: pending.assetId,
      lobbyId: pending.lobbyId as Asset['lobbyId'],
      type: mimeToAssetType(pending.mimeType),
      role: pending.role,
      name: pending.name ?? pending.fileName,
      fileName: pending.fileName,
      uploadedBy: pending.playerId,
      source,
      imageWidth: pending.imageWidth,
      imageHeight: pending.imageHeight,
      createdAt: Date.now(),
    };

    this.lobbyManager.addAsset(pending.lobbyId, asset);
    this.lobbyGateway.broadcastAssetUploaded(pending.lobbyId, asset);

    return { success: true, assetId: pending.assetId };
  }
}
