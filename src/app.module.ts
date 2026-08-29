import { Module } from '@nestjs/common';
import { LobbyModule } from './lobby/lobby.module.js';
import { UploadModule } from './upload/upload.module.js';
import { AppController } from './app.controller.js';
import { AppService } from './app.service.js';

@Module({
  imports: [LobbyModule, UploadModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
