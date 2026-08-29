import { Module } from '@nestjs/common';
import { UploadController } from './upload.controller.js';
import { LobbyModule } from '../lobby/lobby.module.js';

@Module({
  imports: [LobbyModule],
  controllers: [UploadController],
})
export class UploadModule {}
