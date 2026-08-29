import {
  Controller,
  Post,
  Body,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import { LobbyManager } from './lobby-manager.js';

type CreateLobbyBody = { playerName?: unknown };
type JoinLobbyBody = { playerName?: unknown; code?: unknown };

@Controller('lobby')
export class LobbyController {
  constructor(private readonly lobbyManager: LobbyManager) {}

  /**
   * HTTP entry point for lobby creation.
   *
   * Creates the lobby in NestJS (single source of truth) before the client
   * connects via WebSocket. The client then uses RECONNECT_LOBBY to attach
   * its socket to the already-created lobby.
   */
  @Post('create')
  create(@Body() body: CreateLobbyBody) {
    const playerName = body?.playerName;
    if (typeof playerName !== 'string' || !playerName.trim()) {
      throw new HttpException('Player name is required', HttpStatus.BAD_REQUEST);
    }

    const { lobby, playerId } = this.lobbyManager.createLobby(
      playerName,
      '' /* socketId — not available at HTTP time */,
    );

    return {
      lobbyId: lobby.id,
      code: lobby.code,
      playerId,
    };
  }

  /**
   * HTTP entry point for joining a lobby by code.
   *
   * Adds the player to the NestJS lobby before the client connects via
   * WebSocket. The client then uses RECONNECT_LOBBY to attach.
   */
  @Post('join')
  join(@Body() body: JoinLobbyBody) {
    const playerName = body?.playerName;
    const code = body?.code;

    if (typeof playerName !== 'string' || !playerName.trim()) {
      throw new HttpException('Player name is required', HttpStatus.BAD_REQUEST);
    }
    if (typeof code !== 'string' || !code.trim()) {
      throw new HttpException('Lobby code is required', HttpStatus.BAD_REQUEST);
    }

    const result = this.lobbyManager.joinLobby(
      code,
      playerName,
      '' /* socketId — not available at HTTP time */,
    );

    if (!result) {
      throw new HttpException('Lobby not found', HttpStatus.NOT_FOUND);
    }

    return {
      lobbyId: result.lobby.id,
      code: result.lobby.code,
      playerId: result.playerId,
    };
  }
}
