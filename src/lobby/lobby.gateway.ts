import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  OnGatewayConnection,
  OnGatewayDisconnect,
  ConnectedSocket,
  MessageBody,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import * as fs from 'fs';
import { LobbyManager } from './lobby-manager.js';
import type {
  CreateLobbyAction,
  JoinLobbyAction,
  LeaveLobbyAction,
  ReconnectLobbyAction,
  KickPlayerAction,
  StartGameAction,
  CreateObjectRequestAction,
  PointerMoveAction,
  ObjectGrabRequestAction,
  ObjectReleaseRequestAction,
  RequestUploadUrlAction,
  DeleteAssetRequestAction,
  LobbyCreatedEvent,
  LobbyJoinedEvent,
  PlayerJoinedEvent,
  PlayerLeftEvent,
  PlayerKickedEvent,
  HostChangedEvent,
  GameStartedEvent,
  ObjectCreatedEvent,
  ObjectGrabGrantedEvent,
  ObjectGrabRejectedEvent,
  ObjectMovedEvent,
  ObjectReleasedEvent,
  AssetUploadedEvent,
  AssetDeletedEvent,
  UploadUrlReadyEvent,
  ErrorEvent,
  LobbySnapshot,
  CursorPosition,
  CursorsUpdateEvent,
  Asset,
  AssetId,
  ObjectId,
  PlayerId,
  LobbyId,
  EventId,
  SimulationEvent,
} from '../../types/index.js';
import { randomUUID } from 'crypto';

/** Metadata stored per connected socket */
type SocketMeta = {
  playerId: PlayerId | null;
  lobbyId: LobbyId | null;
};

/** Build a SimulationEvent envelope around a payload */
function makeEvent<T>(
  type: string,
  playerId: string,
  sequence: number,
  payload: T,
): SimulationEvent<T> {
  return {
    version: 1,
    id: randomUUID() as EventId,
    sequence,
    type,
    playerId: playerId as PlayerId,
    timestamp: Date.now(),
    payload,
  };
}

@WebSocketGateway({
  cors: {
    origin: process.env.FRONTEND_URL ?? 'http://localhost:8080',
    credentials: true,
  },
})
export class LobbyGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  private readonly server!: Server;

  /** Per-socket metadata */
  private readonly socketMeta = new Map<string, SocketMeta>();

  /** Monotonically increasing sequence counter per lobby */
  private readonly sequences = new Map<string, number>();

  /** Ephemeral cursor positions: lobbyId → (playerId → { position: Vector3; playerName }) */
  private readonly cursorPositions = new Map<
    string,
    Map<string, { position: { x: number; y: number; z: number }; playerName: string }>
  >();

  constructor(private readonly lobbyManager: LobbyManager) {
    // Register callback for host migration events
    this.lobbyManager.setOnHostMigration((lobbyId: string, newHostId: string) => {
      this.broadcastHostChanged(lobbyId, newHostId);
    });
  }

  // ----------------------------------------------------------------
  // Connection lifecycle
  // ----------------------------------------------------------------

  handleConnection(socket: Socket): void {
    this.socketMeta.set(socket.id, { playerId: null, lobbyId: null });
    console.log(`[WebSocket] Client connected: ${socket.id}`);
  }

  handleDisconnect(socket: Socket): void {
    const meta = this.socketMeta.get(socket.id);
    this.socketMeta.delete(socket.id);
    console.log(
      `[WebSocket] Client disconnected: ${socket.id}`,
      meta ? `(player: ${meta.playerId}, lobby: ${meta.lobbyId})` : '(not in lobby)',
    );

    if (!meta?.lobbyId || !meta.playerId) return;

    const updatedLobby = this.lobbyManager.playerDisconnected(
      meta.lobbyId,
      meta.playerId,
    );

    if (!updatedLobby) return;

    const seq = this.nextSeq(meta.lobbyId);

    // Notify remaining players that this player left
    const leftEvent = makeEvent<PlayerLeftEvent>(
      'PLAYER_LEFT',
      meta.playerId,
      seq,
      { type: 'PLAYER_LEFT', playerId: meta.playerId },
    );
    socket.to(meta.lobbyId).emit('event', leftEvent);

    // Notify if host changed
    if (updatedLobby.hostPlayerId !== (meta.playerId as PlayerId)) {
      const hostSeq = this.nextSeq(meta.lobbyId);
      const hostEvent = makeEvent<HostChangedEvent>(
        'HOST_CHANGED',
        meta.playerId,
        hostSeq,
        { type: 'HOST_CHANGED', hostId: updatedLobby.hostPlayerId },
      );
      socket.to(meta.lobbyId).emit('event', hostEvent);
    }

    // Remove cursor for disconnected player and notify remaining players
    this.removeCursorAndBroadcast(meta.lobbyId, meta.playerId, socket);

    // Release all object locks held by this player and notify others
    const releasedIds = this.lobbyManager.releaseAllLocksForPlayer(meta.playerId);
    for (const objectId of releasedIds) {
      const releaseSeq = this.nextSeq(meta.lobbyId);
      const releasedEvent = makeEvent<ObjectReleasedEvent>(
        'OBJECT_RELEASED',
        meta.playerId,
        releaseSeq,
        { type: 'OBJECT_RELEASED', objectId: objectId as ObjectId },
      );
      socket.to(meta.lobbyId).emit('event', releasedEvent);
    }
  }

  // ----------------------------------------------------------------
  // Lobby actions
  // ----------------------------------------------------------------

  @SubscribeMessage('CREATE_LOBBY')
  handleCreateLobby(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    console.log(`[WebSocket] CREATE_LOBBY from ${socket.id}`, payload);
    const data = payload as CreateLobbyAction;

    if (
      !data ||
      typeof data.playerName !== 'string' ||
      !data.playerName.trim()
    ) {
      console.log(`[WebSocket] CREATE_LOBBY validation failed: player name required \n`);
      this.emitError(socket, 'INVALID_ACTION', 'Player name is required');
      return;
    }

    const { lobby, playerId } = this.lobbyManager.createLobby(
      data.playerName,
      socket.id,
      typeof data.lobbyId === 'string' ? data.lobbyId : undefined,
    );

    console.log(
      `[WebSocket] CREATE_LOBBY success: lobby=${lobby.id}, playerId=${playerId}, code=${lobby.code}`,
    );

    this.socketMeta.set(socket.id, {
      playerId,
      lobbyId: lobby.id as LobbyId,
    });

    void socket.join(lobby.id);
    this.sequences.set(lobby.id, 0);

    const seq = this.nextSeq(lobby.id);
    const event = makeEvent<LobbyCreatedEvent>(
      'LOBBY_CREATED',
      playerId,
      seq,
      {
        type: 'LOBBY_CREATED',
        lobby: {
          id: lobby.id,
          code: lobby.code,
          hostPlayerId: lobby.hostPlayerId,
          createdAt: lobby.createdAt,
        },
        player: lobby.players[playerId],
      },
    );

    console.log(`[WebSocket] Emitting LOBBY_CREATED event to ${socket.id} \n`);
    socket.emit('event', event);
  }

  @SubscribeMessage('JOIN_LOBBY')
  handleJoinLobby(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    console.log(`[WebSocket] JOIN_LOBBY from ${socket.id}`, payload);
    const data = payload as JoinLobbyAction;

    if (
      !data ||
      typeof data.playerName !== 'string' ||
      !data.playerName.trim()
    ) {
      console.log(`[WebSocket] JOIN_LOBBY validation failed: player name required \n`);
      this.emitError(socket, 'INVALID_ACTION', 'Player name is required');
      return;
    }
    if (!data.lobbyId || typeof data.lobbyId !== 'string') {
      console.log(`[WebSocket] JOIN_LOBBY validation failed: lobby ID required \n`);
      this.emitError(socket, 'INVALID_ACTION', 'Lobby ID is required');
      return;
    }

    // Support join-by-ID (used when arriving from the Next.js lobby page)
    const lobby = this.lobbyManager.getLobby(data.lobbyId);
    if (!lobby) {
      console.log(`[WebSocket] JOIN_LOBBY failed: lobby not found (${data.lobbyId}) \n`);
      this.emitError(socket, 'LOBBY_NOT_FOUND', 'Lobby not found');
      return;
    }

    const result = this.lobbyManager.joinLobby(
      lobby.code,
      data.playerName,
      socket.id,
    );
    if (!result) {
      console.log(`[WebSocket] JOIN_LOBBY failed: could not add player to lobby \n`);
      this.emitError(socket, 'LOBBY_NOT_FOUND', 'Lobby not found');
      return;
    }

    const { playerId } = result;
    console.log(
      `[WebSocket] JOIN_LOBBY success: lobby=${lobby.id}, playerId=${playerId}, playerCount=${Object.keys(result.lobby.players).length}`,
    );

    this.socketMeta.set(socket.id, {
      playerId,
      lobbyId: lobby.id as LobbyId,
    });

    void socket.join(lobby.id);

    // Send the full snapshot to the joining player
    const snapshot: LobbySnapshot = {
      version: 1,
      timestamp: Date.now(),
      lobby: {
        id: lobby.id,
        code: lobby.code,
        hostPlayerId: lobby.hostPlayerId,
        createdAt: lobby.createdAt,
      },
      players: lobby.players,
      scenes: lobby.scenes,
      objects: lobby.objects,
      assets: lobby.assets,
      groups: lobby.groups,
      stacks: lobby.stacks,
    };

    const joinedSeq = this.nextSeq(lobby.id);
    const joinedEvent = makeEvent<LobbyJoinedEvent>(
      'LOBBY_JOINED',
      playerId,
      joinedSeq,
      {
        type: 'LOBBY_JOINED',
        snapshot,
        player: lobby.players[playerId],
      },
    );
    console.log(`[WebSocket] Emitting LOBBY_JOINED snapshot to ${socket.id}`);
    socket.emit('event', joinedEvent);

    // Notify others that a new player joined
    const joinSeq = this.nextSeq(lobby.id);
    const playerJoinedEvent = makeEvent<PlayerJoinedEvent>(
      'PLAYER_JOINED',
      playerId,
      joinSeq,
      {
        type: 'PLAYER_JOINED',
        player: lobby.players[playerId],
      },
    );
    console.log(
      `[WebSocket] Broadcasting PLAYER_JOINED to lobby ${lobby.id} (${socket.id} joined) \n`,
    );
    socket.to(lobby.id).emit('event', playerJoinedEvent);
  }

  @SubscribeMessage('LEAVE_LOBBY')
  handleLeaveLobby(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    console.log(`[WebSocket] LEAVE_LOBBY from ${socket.id}`, payload);
    const data = payload as LeaveLobbyAction;
    const meta = this.socketMeta.get(socket.id);

    const lobbyId = data?.lobbyId ?? meta?.lobbyId;
    if (!lobbyId) {
      console.log(`[WebSocket] LEAVE_LOBBY failed: no lobby ID found \n`);
      return;
    }

    void socket.leave(lobbyId);

    const updatedLobby = this.lobbyManager.playerDisconnected(
      lobbyId,
      meta?.playerId ?? '',
    );

    this.socketMeta.set(socket.id, { playerId: null, lobbyId: null });

    if (!updatedLobby || !meta?.playerId) {
      console.log(`[WebSocket] LEAVE_LOBBY: no active lobby or player \n`);
      return;
    }

    console.log(
      `[WebSocket] Broadcasting PLAYER_LEFT to lobby ${lobbyId} (${meta.playerId} left)`,
    );

    const seq = this.nextSeq(lobbyId);
    const leftEvent = makeEvent<PlayerLeftEvent>(
      'PLAYER_LEFT',
      meta.playerId,
      seq,
      { type: 'PLAYER_LEFT', playerId: meta.playerId },
    );
    this.server.to(lobbyId).emit('event', leftEvent);

    if (updatedLobby.hostPlayerId !== (meta.playerId as PlayerId)) {
      console.log(
        `[WebSocket] HOST_CHANGED in lobby ${lobbyId}: new host is ${updatedLobby.hostPlayerId}`,
      );
      const hostSeq = this.nextSeq(lobbyId);
      const hostEvent = makeEvent<HostChangedEvent>(
        'HOST_CHANGED',
        meta.playerId,
        hostSeq,
        { type: 'HOST_CHANGED', hostId: updatedLobby.hostPlayerId },
      );
      this.server.to(lobbyId).emit('event', hostEvent);
    }
    console.log();
  }

  @SubscribeMessage('RECONNECT_LOBBY')
  handleReconnect(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    console.log(`[WebSocket] RECONNECT_LOBBY from ${socket.id}`, payload);
    const data = payload as ReconnectLobbyAction;

    if (!data?.lobbyId || !data?.playerId) {
      console.log(`[WebSocket] RECONNECT_LOBBY validation failed: lobbyId and playerId required \n`);
      this.emitError(socket, 'INVALID_ACTION', 'lobbyId and playerId required');
      return;
    }

    const updatedLobby = this.lobbyManager.playerReconnected(
      data.lobbyId,
      data.playerId,
    );

    if (!updatedLobby) {
      console.log(`[WebSocket] RECONNECT_LOBBY failed: lobby not found (${data.lobbyId}) \n`);
      this.emitError(socket, 'LOBBY_NOT_FOUND', 'Lobby not found');
      return;
    }

    console.log(`[WebSocket] RECONNECT_LOBBY success: lobby=${data.lobbyId}, playerId=${data.playerId}`);

    this.socketMeta.set(socket.id, {
      playerId: data.playerId as PlayerId,
      lobbyId: data.lobbyId as LobbyId,
    });

    void socket.join(data.lobbyId);

    // Notify others in the lobby that this player (re)joined
    const player = updatedLobby.players[data.playerId as PlayerId];
    if (player) {
      const joinSeq = this.nextSeq(data.lobbyId);
      const playerJoinedEvent = makeEvent<PlayerJoinedEvent>(
        'PLAYER_JOINED',
        data.playerId,
        joinSeq,
        { type: 'PLAYER_JOINED', player },
      );
      socket.to(data.lobbyId).emit('event', playerJoinedEvent);
    }

    // Send fresh snapshot so client can reconcile
    const snapshot: LobbySnapshot = {
      version: 1,
      timestamp: Date.now(),
      lobby: {
        id: updatedLobby.id,
        code: updatedLobby.code,
        hostPlayerId: updatedLobby.hostPlayerId,
        createdAt: updatedLobby.createdAt,
      },
      players: updatedLobby.players,
      scenes: updatedLobby.scenes,
      objects: updatedLobby.objects,
      assets: updatedLobby.assets,
      groups: updatedLobby.groups,
      stacks: updatedLobby.stacks,
    };

    const seq = this.nextSeq(data.lobbyId);
    const joinedEvent = makeEvent<LobbyJoinedEvent>(
      'LOBBY_JOINED',
      data.playerId,
      seq,
      {
        type: 'LOBBY_JOINED',
        snapshot,
        player: updatedLobby.players[data.playerId as PlayerId],
      },
    );
    console.log(`[WebSocket] Emitting LOBBY_JOINED snapshot to ${socket.id} (reconnect)\n`);
    socket.emit('event', joinedEvent);
  }

  @SubscribeMessage('KICK_PLAYER')
  handleKickPlayer(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    console.log(`[WebSocket] KICK_PLAYER from ${socket.id}`, payload);
    const data = payload as KickPlayerAction;
    const meta = this.socketMeta.get(socket.id);

    if (!meta?.lobbyId || !meta.playerId) {
      console.log(`[WebSocket] KICK_PLAYER failed: player not in a lobby \n`);
      this.emitError(socket, 'INVALID_ACTION', 'You are not in a lobby');
      return;
    }

    const lobby = this.lobbyManager.getLobby(meta.lobbyId);
    if (!lobby) {
      console.log(`[WebSocket] KICK_PLAYER failed: lobby not found \n`);
      this.emitError(socket, 'LOBBY_NOT_FOUND', 'Lobby not found');
      return;
    }

    // Only the host can kick players
    if (lobby.hostPlayerId !== meta.playerId) {
      console.log(`[WebSocket] KICK_PLAYER denied: ${meta.playerId} is not host \n`);
      this.emitError(socket, 'PERMISSION_DENIED', 'Only the host can kick players');
      return;
    }

    const playerToKick = data?.playerId;
    if (!playerToKick || playerToKick === meta.playerId) {
      console.log(`[WebSocket] KICK_PLAYER validation failed: invalid target player \n`);
      this.emitError(socket, 'INVALID_ACTION', 'Cannot kick this player');
      return;
    }

    // Check that the player to kick exists in the lobby
    if (!lobby.players[playerToKick as PlayerId]) {
      console.log(`[WebSocket] KICK_PLAYER failed: player ${playerToKick} not in lobby \n`);
      this.emitError(socket, 'INVALID_ACTION', 'Player not found in lobby');
      return;
    }

    const wasHostChanged = lobby.hostPlayerId !== (playerToKick as PlayerId);
    const updatedLobby = this.lobbyManager.kickPlayer(meta.lobbyId, playerToKick);

    if (!updatedLobby) {
      console.log(`[WebSocket] KICK_PLAYER failed: could not update lobby \n`);
      this.emitError(socket, 'INTERNAL_ERROR', 'Failed to kick player');
      return;
    }

    console.log(
      `[WebSocket] Successfully kicked player ${playerToKick} from lobby ${meta.lobbyId}`,
    );

    // Broadcast PLAYER_KICKED event to all players in the lobby
    const seq = this.nextSeq(meta.lobbyId);
    const kickedEvent = makeEvent<PlayerKickedEvent>(
      'PLAYER_KICKED',
      meta.playerId,
      seq,
      { type: 'PLAYER_KICKED', playerId: playerToKick as PlayerId },
    );
    this.server.to(meta.lobbyId).emit('event', kickedEvent);

    // Notify if host changed (kicked player was host)
    if (!wasHostChanged && updatedLobby.hostPlayerId !== lobby.hostPlayerId) {
      console.log(
        `[WebSocket] HOST_CHANGED in lobby ${meta.lobbyId}: new host is ${updatedLobby.hostPlayerId}`,
      );
      const hostSeq = this.nextSeq(meta.lobbyId);
      const hostEvent = makeEvent<HostChangedEvent>(
        'HOST_CHANGED',
        meta.playerId,
        hostSeq,
        { type: 'HOST_CHANGED', hostId: updatedLobby.hostPlayerId },
      );
      this.server.to(meta.lobbyId).emit('event', hostEvent);
    }
    console.log();
  }

  @SubscribeMessage('START_GAME')
  handleStartGame(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    console.log(`[WebSocket] START_GAME from ${socket.id}`, payload);
    const data = payload as StartGameAction;
    const meta = this.socketMeta.get(socket.id);

    if (!meta?.lobbyId || !meta.playerId) {
      console.log(`[WebSocket] START_GAME failed: player not in a lobby \n`);
      this.emitError(socket, 'INVALID_ACTION', 'You are not in a lobby');
      return;
    }

    const lobby = this.lobbyManager.getLobby(meta.lobbyId);
    if (!lobby) {
      console.log(`[WebSocket] START_GAME failed: lobby not found \n`);
      this.emitError(socket, 'LOBBY_NOT_FOUND', 'Lobby not found');
      return;
    }

    // Only the host can start the game
    if (lobby.hostPlayerId !== meta.playerId) {
      console.log(`[WebSocket] START_GAME denied: ${meta.playerId} is not host \n`);
      this.emitError(socket, 'PERMISSION_DENIED', 'Only the host can start the game');
      return;
    }

    console.log(`[WebSocket] Broadcasting GAME_STARTED to lobby ${meta.lobbyId}`);
    const seq = this.nextSeq(meta.lobbyId);
    const gameStartedEvent = makeEvent<GameStartedEvent>(
      'GAME_STARTED',
      meta.playerId,
      seq,
      { type: 'GAME_STARTED', lobbyId: meta.lobbyId, lobbyCode: lobby.code },
    );
    this.server.to(meta.lobbyId).emit('event', gameStartedEvent);
    console.log();
  }

  // ----------------------------------------------------------------
  // Asset actions
  // ----------------------------------------------------------------

  @SubscribeMessage('REQUEST_UPLOAD_URL')
  handleRequestUploadUrl(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    const data = payload as RequestUploadUrlAction;
    const meta = this.socketMeta.get(socket.id);

    if (!meta?.lobbyId || !meta.playerId) {
      this.emitError(socket, 'INVALID_ACTION', 'You are not in a lobby');
      return;
    }

    if (!data?.role || !data.fileName || !data.mimeType) {
      this.emitError(socket, 'INVALID_ACTION', 'role, fileName, and mimeType are required');
      return;
    }

    const nestjsBaseUrl = process.env.NESTJS_BASE_URL ?? 'http://localhost:3001';

    const { assetId, uploadUrl } = this.lobbyManager.generateUploadToken(
      meta.lobbyId,
      meta.playerId,
      data.role,
      data.name,
      data.fileName,
      data.mimeType,
      nestjsBaseUrl,
      data.imageWidth,
      data.imageHeight,
    );

    const seq = this.nextSeq(meta.lobbyId);
    const readyEvent = makeEvent<UploadUrlReadyEvent>(
      'UPLOAD_URL_READY',
      meta.playerId,
      seq,
      { type: 'UPLOAD_URL_READY', assetId, uploadUrl },
    );
    socket.emit('event', readyEvent);
  }

  @SubscribeMessage('DELETE_ASSET_REQUEST')
  handleDeleteAsset(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    const data = payload as DeleteAssetRequestAction;
    const meta = this.socketMeta.get(socket.id);

    if (!meta?.lobbyId || !meta.playerId) {
      this.emitError(socket, 'INVALID_ACTION', 'You are not in a lobby');
      return;
    }

    if (!data?.assetId) {
      this.emitError(socket, 'INVALID_ACTION', 'assetId is required');
      return;
    }

    const uploadsDir = process.cwd() + '/uploads';
    const result = this.lobbyManager.deleteAsset(
      meta.lobbyId,
      data.assetId as string,
      meta.playerId,
      uploadsDir,
    );

    if (!result) {
      this.emitError(socket, 'PERMISSION_DENIED', 'Cannot delete this asset');
      return;
    }

    // Delete file from disk asynchronously
    fs.unlink(result.filePath, (err) => {
      if (err && err.code !== 'ENOENT') {
        console.error(`[WebSocket] Failed to delete asset file ${result.filePath}:`, err);
      }
    });

    const seq = this.nextSeq(meta.lobbyId);
    const deletedEvent = makeEvent<AssetDeletedEvent>(
      'ASSET_DELETED',
      meta.playerId,
      seq,
      { type: 'ASSET_DELETED', assetId: data.assetId as AssetId },
    );
    this.server.to(meta.lobbyId).emit('event', deletedEvent);
  }

  /**
   * Called by UploadController after a file is successfully stored on disk.
   * Broadcasts ASSET_UPLOADED to all players in the lobby.
   */
  broadcastAssetUploaded(lobbyId: string, asset: Asset): void {
    const seq = this.nextSeq(lobbyId);
    const event = makeEvent<AssetUploadedEvent>(
      'ASSET_UPLOADED',
      asset.uploadedBy as string,
      seq,
      { type: 'ASSET_UPLOADED', asset },
    );
    this.server.to(lobbyId).emit('event', event);
  }

  // ----------------------------------------------------------------
  // Object interaction actions
  // ----------------------------------------------------------------

  @SubscribeMessage('CREATE_OBJECT_REQUEST')
  handleCreateObjectRequest(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    const data = payload as CreateObjectRequestAction;
    const meta = this.socketMeta.get(socket.id);

    if (!meta?.lobbyId || !meta.playerId) {
      this.emitError(socket, 'INVALID_ACTION', 'You are not in a lobby');
      return;
    }

    if (
      !data?.sceneId ||
      !data.position ||
      typeof data.position.x !== 'number' ||
      typeof data.position.y !== 'number' ||
      typeof data.position.z !== 'number' ||
      !data.source ||
      (data.source.kind !== 'builtin' && data.source.kind !== 'asset')
    ) {
      this.emitError(socket, 'INVALID_ACTION', 'Invalid create object request');
      return;
    }

    if (
      data.source.kind === 'builtin' &&
      (data.source.key !== 'pawn' && data.source.key !== 'mat')
    ) {
      this.emitError(socket, 'INVALID_ACTION', 'Unknown built-in resource');
      return;
    }

    if (
      data.source.kind === 'asset' &&
      typeof data.source.assetId !== 'string'
    ) {
      this.emitError(socket, 'INVALID_ACTION', 'assetId is required');
      return;
    }

    const result = this.lobbyManager.createObject(meta.lobbyId, meta.playerId, data);
    if (!result.ok) {
      this.emitError(socket, result.code, result.message);
      return;
    }

    const seq = this.nextSeq(meta.lobbyId);
    const event = makeEvent<ObjectCreatedEvent>(
      'OBJECT_CREATED',
      meta.playerId,
      seq,
      { type: 'OBJECT_CREATED', object: result.object },
    );
    this.server.to(meta.lobbyId).emit('event', event);
  }

  @SubscribeMessage('OBJECT_GRAB_REQUEST')
  handleObjectGrabRequest(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    const data = payload as ObjectGrabRequestAction;
    const meta = this.socketMeta.get(socket.id);
    if (!meta?.lobbyId || !meta.playerId) return;

    const objectId = data?.objectId;
    if (!objectId || typeof objectId !== 'string') {
      this.emitError(socket, 'INVALID_ACTION', 'objectId is required');
      return;
    }

    const granted = this.lobbyManager.tryAcquireLock(objectId, meta.playerId, meta.lobbyId);

    if (granted) {
      const seq = this.nextSeq(meta.lobbyId);
      const event = makeEvent<ObjectGrabGrantedEvent>(
        'OBJECT_GRAB_GRANTED',
        meta.playerId,
        seq,
        { type: 'OBJECT_GRAB_GRANTED', objectId: objectId as ObjectId, playerId: meta.playerId },
      );
      // Broadcast to all (remote players see who grabbed which object)
      this.server.to(meta.lobbyId).emit('event', event);
    } else {
      const lockHolder = this.lobbyManager.getLockHolder(objectId) ?? meta.playerId;
      const seq = this.nextSeq(meta.lobbyId);
      const event = makeEvent<ObjectGrabRejectedEvent>(
        'OBJECT_GRAB_REJECTED',
        meta.playerId,
        seq,
        {
          type: 'OBJECT_GRAB_REJECTED',
          objectId: objectId as ObjectId,
          ownerId: lockHolder as PlayerId,
        },
      );
      // Only to the requester
      socket.emit('event', event);
    }
  }

  @SubscribeMessage('POINTER_MOVE')
  handlePointerMove(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    const meta = this.socketMeta.get(socket.id);
    if (!meta?.lobbyId || !meta.playerId) return;

    const data = payload as PointerMoveAction;
    const { position } = data ?? {};
    if (
      !position ||
      typeof position.x !== 'number' ||
      typeof position.y !== 'number' ||
      typeof position.z !== 'number'
    ) {
      return;
    }

    const lobby = this.lobbyManager.getLobby(meta.lobbyId);
    if (!lobby) return;

    // Always upsert cursor position, including while dragging.
    if (!this.cursorPositions.has(meta.lobbyId)) {
      this.cursorPositions.set(meta.lobbyId, new Map());
    }
    const lobbyCursors = this.cursorPositions.get(meta.lobbyId)!;
    const playerName = lobby.players[meta.playerId]?.displayName ?? 'Unknown';
    lobbyCursors.set(meta.playerId, {
      position: { x: position.x, y: position.y, z: position.z },
      playerName,
    });
    this.broadcastCursors(meta.lobbyId);

    // If objectId is provided, attempt authoritative move as lock-holder.
    if (!data.objectId || typeof data.objectId !== 'string') return;

    const moved = this.lobbyManager.moveObject(
      data.objectId,
      meta.playerId,
      meta.lobbyId,
      position,
    );
    if (!moved) return; // not lock-holder — ignore silently

    const seq = this.nextSeq(meta.lobbyId);
    const event = makeEvent<ObjectMovedEvent>(
      'OBJECT_MOVED',
      meta.playerId,
      seq,
      { type: 'OBJECT_MOVED', objectId: data.objectId as ObjectId, position },
    );
    this.server.to(meta.lobbyId).emit('event', event);
  }

  @SubscribeMessage('OBJECT_RELEASE_REQUEST')
  handleObjectReleaseRequest(
    @ConnectedSocket() socket: Socket,
    @MessageBody() payload: unknown,
  ): void {
    const data = payload as ObjectReleaseRequestAction;
    const meta = this.socketMeta.get(socket.id);
    if (!meta?.lobbyId || !meta.playerId) return;

    const objectId = data?.objectId;
    if (!objectId || typeof objectId !== 'string') return;

    this.lobbyManager.releaseLock(objectId, meta.playerId);

    const seq = this.nextSeq(meta.lobbyId);
    const event = makeEvent<ObjectReleasedEvent>(
      'OBJECT_RELEASED',
      meta.playerId,
      seq,
      { type: 'OBJECT_RELEASED', objectId: objectId as ObjectId },
    );
    this.server.to(meta.lobbyId).emit('event', event);
  }

  // ----------------------------------------------------------------
  // Cursor tracking (fed by POINTER_MOVE)
  // ----------------------------------------------------------------

  private broadcastCursors(lobbyId: string): void {
    const lobbyCursors = this.cursorPositions.get(lobbyId);
    const cursors: CursorPosition[] = lobbyCursors
      ? Array.from(lobbyCursors.entries()).map(([pid, cur]) => ({
          playerId: pid as PlayerId,
          playerName: cur.playerName,
          position: cur.position,
        }))
      : [];

    const event: CursorsUpdateEvent = { type: 'CURSORS_UPDATE', cursors };
    this.server.to(lobbyId).emit('cursors_update', event);
  }

  private removeCursorAndBroadcast(
    lobbyId: string,
    playerId: string,
    socket: Socket,
  ): void {
    const lobbyCursors = this.cursorPositions.get(lobbyId);
    if (!lobbyCursors) return;
    lobbyCursors.delete(playerId);

    const cursors: CursorPosition[] = Array.from(lobbyCursors.entries()).map(
      ([pid, cur]) => ({
        playerId: pid as PlayerId,
        playerName: cur.playerName,
        position: cur.position,
      }),
    );
    const event: CursorsUpdateEvent = { type: 'CURSORS_UPDATE', cursors };
    socket.to(lobbyId).emit('cursors_update', event);
  }

  // ----------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------

  private nextSeq(lobbyId: string): number {
    const current = this.sequences.get(lobbyId) ?? 0;
    const next = current + 1;
    this.sequences.set(lobbyId, next);
    return next;
  }

  private emitError(
    socket: Socket,
    code: ErrorEvent['code'],
    message: string,
  ): void {
    console.log(`[WebSocket] ERROR to ${socket.id}: ${code} — ${message}`);
    const payload: ErrorEvent = { type: 'ERROR', code, message };
    socket.emit('event', makeEvent('ERROR', '', 0, payload));
  }

  private broadcastHostChanged(lobbyId: string, newHostId: string): void {
    console.log(
      `[WebSocket] Broadcasting HOST_CHANGED to lobby ${lobbyId}: new host is ${newHostId}`,
    );
    const seq = this.nextSeq(lobbyId);
    const hostEvent = makeEvent<HostChangedEvent>(
      'HOST_CHANGED',
      newHostId,
      seq,
      { type: 'HOST_CHANGED', hostId: newHostId as PlayerId },
    );
    this.server.to(lobbyId).emit('event', hostEvent);
  }
}
