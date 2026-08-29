import { Module } from '@nestjs/common';
import { LobbyManager } from './lobby-manager.js';
import { LobbyGateway } from './lobby.gateway.js';
import { LobbyController } from './lobby.controller.js';

@Module({
  controllers: [LobbyController],
  providers: [LobbyManager, LobbyGateway],
  exports: [LobbyManager, LobbyGateway],
})
export class LobbyModule {}
