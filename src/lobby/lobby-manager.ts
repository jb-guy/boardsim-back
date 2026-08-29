import { Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import type {
  LobbyState,
  LobbyId,
  PlayerId,
  SceneId,
  ObjectId,
  AssetId,
  AssetRole,
  BuiltinResourceKey,
  Player,
  Scene,
  Asset,
  GameObject,
  Vector3,
  CreateObjectRequestAction,
} from '../../types/index.js';

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function generateLobbyCode(): string {
  return Array.from(
    { length: 6 },
    () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)],
  ).join('');
}

// ----------------------------------------------------------------
// Default placeholder objects seeded for every new lobby.
// Mirror of the client mock data — replaced by real assets in Phase 2.
// ----------------------------------------------------------------

const CARD_DEFS = [
  { id: 'card-1', x: 0, z: 0, color: '#06b6d4' },
  { id: 'card-2', x: 2.5, z: 1, color: '#10b981' },
  { id: 'card-3', x: -2.5, z: -1, color: '#8b5cf6' },
  { id: 'card-4', x: 1.5, z: -3.5, color: '#f59e0b' },
  { id: 'card-5', x: -1, z: 3.5, color: '#94a3b8' },
] as const;

function makeDefaultObjects(
  sceneId: SceneId,
  now: number,
): Record<string, GameObject> {
  const objects: Record<string, GameObject> = {};
  for (const { id, x, z, color } of CARD_DEFS) {
    objects[id] = {
      id: id as ObjectId,
      sceneId,
      transform: {
        position: { x, y: 0.025, z },
        rotation: { x: 0, y: 0, z: 0 },
        scale: { x: 1, y: 1, z: 1 },
      },
      visibility: { mode: 'public' },
      capabilities: {
        selectable: true,
        draggable: true,
        stackable: true,
        flippable: true,
        lockable: true,
        groupable: true,
      },
      lock: null,
      metadata: { color },
      createdAt: now,
      updatedAt: now,
    };
  }
  return objects;
}

type BuiltinSpawnDefinition = {
  key: BuiltinResourceKey;
  role: AssetRole;
  label: string;
  color: string;
  scale: Vector3;
};

type CreateObjectResult =
  | {
      ok: true;
      object: GameObject;
    }
  | {
      ok: false;
      code: 'LOBBY_NOT_FOUND' | 'INVALID_OBJECT' | 'ASSET_NOT_FOUND';
      message: string;
    };

const BUILTIN_SPAWN_DEFINITIONS: Record<BuiltinResourceKey, BuiltinSpawnDefinition> = {
  pawn: {
    key: 'pawn',
    role: 'piece',
    label: 'Pawn',
    color: '#f59e0b',
    scale: { x: 1, y: 1, z: 1 },
  },
  mat: {
    key: 'mat',
    role: 'board',
    label: 'Mat',
    color: '#10b981',
    scale: { x: 3.8, y: 1, z: 2.6 },
  },
};

function capabilitiesForRole(role: AssetRole) {
  return {
    selectable: true,
    draggable: true,
    stackable: role === 'card' || role === 'token',
    flippable: role === 'card' || role === 'board',
    lockable: true,
    groupable: true,
  };
}

function scaleForImage(
  imageWidth: number | undefined,
  imageHeight: number | undefined,
): Vector3 {
  if (!imageWidth || !imageHeight) {
    // Fallback to card-like proportions if dimensions unavailable
    return { x: 1.2, y: 1, z: 1.6 };
  }

  // Calculate aspect ratio and scale to fit within standard card bounds
  // Card standard: 1.2 × 1.6 (X × Z)
  const aspectRatio = imageWidth / imageHeight;
  const cardAspectRatio = 1.2 / 1.6;

  if (aspectRatio > cardAspectRatio) {
    // Image is wider: constrain by X
    return { x: 1.2, y: 1, z: 1.2 / aspectRatio };
  } else {
    // Image is taller: constrain by Z
    return { x: 1.6 * aspectRatio, y: 1, z: 1.6 };
  }
}

export type PendingUpload = {
  token: string;
  assetId: AssetId;
  lobbyId: string;
  playerId: PlayerId;
  role: AssetRole;
  name: string | undefined;
  fileName: string;
  mimeType: string;
  imageWidth?: number;
  imageHeight?: number;
  expiresAt: number;
};

export type CreateLobbyResult = {
  lobby: LobbyState;
  playerId: PlayerId;
};

export type JoinLobbyResult = {
  lobby: LobbyState;
  playerId: PlayerId;
};

@Injectable()
export class LobbyManager {
  /** All active lobbies, keyed by lobby ID */
  private readonly lobbies = new Map<string, LobbyState>();

  /** Index from lobby code → lobby ID for fast join lookups */
  private readonly codeIndex = new Map<string, string>();

  /** Cleanup timers: lobby ID → timer handle */
  private readonly cleanupTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();

  /** Host migration timers: (lobbyId:playerId) → timer handle */
  private readonly hostMigrationTimers = new Map<
    string,
    ReturnType<typeof setTimeout>
  >();

  /** Object locks: objectId → playerId currently holding the lock */
  private readonly objectLocks = new Map<string, string>();

  /** Callback invoked when host migration completes */
  private onHostMigration?: (lobbyId: string, newHostId: PlayerId) => void;

  /** How long (ms) an empty lobby lives before being deleted (10 min) */
  private static readonly EMPTY_LOBBY_TTL_MS = 10 * 60 * 1000;

  /** How long (ms) to wait before migrating host after disconnect (5 sec) */
  private static readonly HOST_MIGRATION_DELAY_MS = 5 * 1000;

  // ----------------------------------------------------------------
  // Pending upload tokens
  //
  // A player emits REQUEST_UPLOAD_URL → server stores a PendingUpload
  // keyed by a one-time UUID token and returns a signed upload URL to
  // the requesting socket only. The UploadController consumes the token
  // when the file arrives.
  // ----------------------------------------------------------------

  private readonly pendingUploads = new Map<string, PendingUpload>();

  /** Token TTL: 5 minutes */
  private static readonly UPLOAD_TOKEN_TTL_MS = 5 * 60 * 1000;

  // ----------------------------------------------------------------
  // Public API
  // ----------------------------------------------------------------

  /**
   * Register a callback to be invoked when host migration completes.
   * Used by the gateway to broadcast HOST_CHANGED events.
   */
  setOnHostMigration(callback: (lobbyId: string, newHostId: PlayerId) => void): void {
    this.onHostMigration = callback;
  }

  createLobby(
    hostName: string,
    socketId: string,
    existingLobbyId?: string,
  ): CreateLobbyResult {
    const lobbyId = (existingLobbyId ?? randomUUID()) as LobbyId;
    const playerId = randomUUID() as PlayerId;
    const sceneId = randomUUID() as SceneId;
    const now = Date.now();

    const host: Player = {
      id: playerId,
      displayName: hostName.trim(),
      connected: true,
      joinedAt: now,
    };

    const defaultScene: Scene = {
      id: sceneId,
      name: 'Main',
      type: 'public',
      objectIds: ['card-1', 'card-2', 'card-3', 'card-4', 'card-5'] as ObjectId[],
    };

    const defaultObjects = makeDefaultObjects(sceneId, now);

    const lobby: LobbyState = {
      id: lobbyId,
      code: this.uniqueCode(),
      hostPlayerId: playerId,
      createdAt: now,
      players: { [playerId]: host } as Record<PlayerId, Player>,
      scenes: { [sceneId]: defaultScene } as Record<SceneId, Scene>,
      objects: defaultObjects as Record<ObjectId, GameObject>,
      assets: {},
      groups: {},
      stacks: {},
    };

    console.log(`LobbyManager: created lobby ${lobbyId} with host ${hostName} (${playerId}), socket ${socketId}), code ${lobby.code}`);

    this.lobbies.set(lobbyId, lobby);
    this.codeIndex.set(lobby.code, lobbyId);

    return { lobby, playerId };
  }

  joinLobby(
    code: string,
    playerName: string,
    socketId: string,
  ): JoinLobbyResult | null {
    const normalizedCode = code.trim().toUpperCase();
    const lobbyId = this.codeIndex.get(normalizedCode);
    if (!lobbyId) return null;

    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return null;

    const playerId = randomUUID() as PlayerId;
    const now = Date.now();

    const player: Player = {
      id: playerId,
      displayName: playerName.trim(),
      connected: true,
      joinedAt: now,
    };

    console.log(`LobbyManager: player ${playerName} (${playerId}) joining lobby ${lobbyId}`);

    (lobby.players as Record<string, Player>)[playerId] = player;

    // Cancel any pending cleanup since a new player joined
    this.cancelCleanup(lobbyId);

    return { lobby, playerId };
  }

  getLobby(id: string): LobbyState | undefined {
    return this.lobbies.get(id);
  }

  /**
   * Reserve a one-time upload token for the given player + lobby.
   * Returns the token, assetId, and the upload URL the client should POST to.
   */
  generateUploadToken(
    lobbyId: string,
    playerId: string,
    role: AssetRole,
    name: string | undefined,
    fileName: string,
    mimeType: string,
    nestjsBaseUrl: string,
    imageWidth?: number,
    imageHeight?: number,
  ): { token: string; assetId: AssetId; uploadUrl: string } {
    const token = randomUUID();
    const assetId = randomUUID() as AssetId;
    const expiresAt = Date.now() + LobbyManager.UPLOAD_TOKEN_TTL_MS;

    this.pendingUploads.set(token, {
      token,
      assetId,
      lobbyId,
      playerId: playerId as PlayerId,
      role,
      name,
      fileName,
      mimeType,
      imageWidth,
      imageHeight,
      expiresAt,
    });

    // Auto-expire token
    setTimeout(() => {
      this.pendingUploads.delete(token);
    }, LobbyManager.UPLOAD_TOKEN_TTL_MS);

    const uploadUrl = `${nestjsBaseUrl}/upload/${token}`;
    return { token, assetId, uploadUrl };
  }

  /**
   * Consume a pending upload token. Returns the metadata and removes the token.
   * Returns null if the token is unknown or has expired.
   */
  consumeUploadToken(token: string): PendingUpload | null {
    const pending = this.pendingUploads.get(token);
    if (!pending) return null;
    if (Date.now() > pending.expiresAt) {
      this.pendingUploads.delete(token);
      return null;
    }
    this.pendingUploads.delete(token);
    return pending;
  }

  /**
   * Add a fully-uploaded asset to the lobby state.
   */
  addAsset(lobbyId: string, asset: Asset): void {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return;
    (lobby.assets as Record<string, Asset>)[asset.id] = asset;
  }

  createObject(
    lobbyId: string,
    _playerId: string,
    request: CreateObjectRequestAction,
  ): CreateObjectResult {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) {
      return {
        ok: false,
        code: 'LOBBY_NOT_FOUND',
        message: 'Lobby not found',
      };
    }

    const scene = lobby.scenes[request.sceneId];
    if (!scene) {
      return {
        ok: false,
        code: 'INVALID_OBJECT',
        message: 'Scene not found',
      };
    }

    const objectId = randomUUID() as ObjectId;
    const now = Date.now();

    let role: AssetRole;
    let label: string;
    let color: string | undefined;
    let scale: Vector3;
    let assetId: AssetId | undefined;
    let builtinKey: BuiltinResourceKey | undefined;

    if (request.source.kind === 'builtin') {
      const builtin = BUILTIN_SPAWN_DEFINITIONS[request.source.key];
      if (!builtin) {
        return {
          ok: false,
          code: 'INVALID_OBJECT',
          message: 'Unknown built-in resource',
        };
      }

      role = builtin.role;
      label = builtin.label;
      color = builtin.color;
      scale = builtin.scale;
      builtinKey = builtin.key;
    } else {
      const asset = lobby.assets[request.source.assetId];
      if (!asset) {
        return {
          ok: false,
          code: 'ASSET_NOT_FOUND',
          message: 'Asset not found',
        };
      }
      if (asset.type !== 'image') {
        return {
          ok: false,
          code: 'INVALID_OBJECT',
          message: 'Only image assets can be instantiated',
        };
      }

      role = asset.role;
      label = asset.name;
      // Use aspect-ratio-aware scale for images; fall back to role scale for others
      scale = scaleForImage(asset.imageWidth, asset.imageHeight);
      assetId = asset.id;
    }

    const object: GameObject = {
      id: objectId,
      sceneId: request.sceneId,
      assetId,
      transform: {
        position: {
          x: request.position.x,
          y: 0.025,
          z: request.position.z,
        },
        rotation: { x: 0, y: 0, z: 0 },
        scale,
      },
      visibility: { mode: 'public' },
      capabilities: capabilitiesForRole(role),
      lock: null,
      metadata: {
        resourceRole: role,
        label,
        ...(color ? { color } : {}),
        ...(builtinKey ? { builtinKey } : {}),
      },
      createdAt: now,
      updatedAt: now,
    };

    (lobby.objects as Record<string, GameObject>)[objectId] = object;
    scene.objectIds = [...scene.objectIds, objectId];

    return { ok: true, object };
  }

  /**
   * Delete an asset from lobby state.
   * Only the uploader or the host may delete an asset.
   * Returns the asset's file path on success, null on permission denied or not found.
   */
  deleteAsset(
    lobbyId: string,
    assetId: string,
    requesterId: string,
    uploadsDir: string,
  ): { filePath: string; asset: Asset } | null {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return null;

    const asset = (lobby.assets as Record<string, Asset>)[assetId];
    if (!asset) return null;

    const isHost = lobby.hostPlayerId === (requesterId as PlayerId);
    const isUploader = asset.uploadedBy === (requesterId as PlayerId);
    if (!isHost && !isUploader) return null;

    delete (lobby.assets as Record<string, Asset>)[assetId];

    // Derive file path from the source URL suffix
    const filePath = path.join(uploadsDir, lobbyId, `${assetId}${path.extname(asset.fileName)}`);
    return { filePath, asset };
  }

  // ----------------------------------------------------------------
  // Object lock management
  // ----------------------------------------------------------------

  /**
   * Atomically acquire a lock on an object.
   * Fails (returns false) if the object is already locked by another player.
   * A player re-locking an object they already hold always succeeds.
   */
  tryAcquireLock(objectId: string, playerId: string, lobbyId: string): boolean {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return false;
    if (!(lobby.objects as Record<string, unknown>)[objectId]) return false;

    const current = this.objectLocks.get(objectId);
    if (current && current !== playerId) return false;

    this.objectLocks.set(objectId, playerId);
    return true;
  }

  /**
   * Release a lock. No-op if the caller doesn't hold the lock.
   */
  releaseLock(objectId: string, playerId: string): void {
    if (this.objectLocks.get(objectId) === playerId) {
      this.objectLocks.delete(objectId);
    }
  }

  /**
   * Move an object's authoritative position.
   * Only the current lock-holder may move the object.
   * Returns true on success.
   */
  moveObject(
    objectId: string,
    playerId: string,
    lobbyId: string,
    position: Vector3,
  ): boolean {
    if (this.objectLocks.get(objectId) !== playerId) return false;

    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return false;

    const obj = (lobby.objects as Record<string, GameObject>)[objectId];
    if (!obj) return false;

    obj.transform.position = {
      x: position.x,
      y: obj.transform.position.y,
      z: position.z,
    };
    obj.updatedAt = Date.now();
    return true;
  }

  /**
   * Release ALL locks held by a player (called on disconnect).
   * Returns the list of objectIds whose locks were released.
   */
  releaseAllLocksForPlayer(playerId: string): string[] {
    const released: string[] = [];
    for (const [objectId, holder] of this.objectLocks.entries()) {
      if (holder === playerId) {
        this.objectLocks.delete(objectId);
        released.push(objectId);
      }
    }
    return released;
  }

  /** Return the playerId currently holding a lock, or undefined. */
  getLockHolder(objectId: string): string | undefined {
    return this.objectLocks.get(objectId);
  }

  // ----------------------------------------------------------------
  // Player lifecycle
  // ----------------------------------------------------------------

  /**
   * Mark a player as disconnected. If the lobby is now empty, schedule
   * deletion after EMPTY_LOBBY_TTL_MS. If the disconnected player was the
   * host, schedule host migration after HOST_MIGRATION_DELAY_MS to allow
   * quick reconnection.
   *
   * Returns the updated lobby, or null if it was already deleted.
   */
  playerDisconnected(
    lobbyId: string,
    playerId: string,
  ): LobbyState | null {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return null;

    const player = (lobby.players as Record<string, Player>)[playerId];
    if (player) {
      player.connected = false;
    }

    // Host migration with delay
    if (lobby.hostPlayerId === (playerId as PlayerId)) {
      this.scheduleHostMigration(lobbyId, playerId);
    }

    const hasConnected = Object.values(lobby.players).some(
      (p) => p.connected,
    );

    if (!hasConnected) {
      this.scheduleCleanup(lobbyId);
    }

    return lobby;
  }

  /**
   * Re-mark a player as connected (reconnect case).
   * If this is the host reconnecting, cancel any pending host migration.
   */
  playerReconnected(lobbyId: string, playerId: string): LobbyState | null {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return null;

    const player = (lobby.players as Record<string, Player>)[playerId];
    if (player) {
      player.connected = true;
    }

    // Cancel host migration if the host reconnects
    if (lobby.hostPlayerId === (playerId as PlayerId)) {
      this.cancelHostMigration(lobbyId, playerId);
    }

    this.cancelCleanup(lobbyId);
    return lobby;
  }

  /**
   * Remove a player from a lobby (kicked by host).
   * If the kicked player was the host, migrate host to another connected player.
   *
   * Returns the updated lobby, or null if it was already deleted.
   */
  kickPlayer(
    lobbyId: string,
    playerId: string,
  ): LobbyState | null {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return null;

    // Remove player from lobby
    const players = lobby.players as Record<string, Player>;
    delete players[playerId];

    // Host migration if kicked player was host
    if (lobby.hostPlayerId === (playerId as PlayerId)) {
      const nextHost = Object.values(players).find((p) => p.connected);
      if (nextHost) {
        lobby.hostPlayerId = nextHost.id;
      } else {
        // No other connected players, but keep the lobby alive
        // The remaining disconnected players or empty lobby will be cleaned up later
      }
    }

    // Schedule cleanup if no connected players remain
    const hasConnected = Object.values(players).some((p) => p.connected);
    if (!hasConnected && Object.keys(players).length === 0) {
      this.scheduleCleanup(lobbyId);
    }

    return lobby;
  }

  // ----------------------------------------------------------------
  // Internal helpers
  // ----------------------------------------------------------------

  private scheduleHostMigration(lobbyId: string, playerId: string): void {
    const timerId = `${lobbyId}:${playerId}`;
    
    // Cancel any existing migration timer for this host
    const existingTimer = this.hostMigrationTimers.get(timerId);
    if (existingTimer !== undefined) {
      clearTimeout(existingTimer);
    }

    const timer = setTimeout(() => {
      this.performHostMigration(lobbyId, playerId);
      this.hostMigrationTimers.delete(timerId);
    }, LobbyManager.HOST_MIGRATION_DELAY_MS);

    this.hostMigrationTimers.set(timerId, timer);
  }

  private cancelHostMigration(lobbyId: string, playerId: string): void {
    const timerId = `${lobbyId}:${playerId}`;
    const timer = this.hostMigrationTimers.get(timerId);
    if (timer !== undefined) {
      clearTimeout(timer);
      this.hostMigrationTimers.delete(timerId);
    }
  }

  private performHostMigration(lobbyId: string, playerId: string): void {
    const lobby = this.lobbies.get(lobbyId);
    if (!lobby) return;

    // Only migrate if this player is still the host
    if (lobby.hostPlayerId !== (playerId as PlayerId)) {
      return;
    }

    const nextHost = Object.values(lobby.players).find(
      (p) => p.id !== (playerId as PlayerId) && p.connected,
    );
    if (nextHost) {
      lobby.hostPlayerId = nextHost.id;
      console.log(`LobbyManager: host migrated from ${playerId} to ${nextHost.id} in lobby ${lobbyId}`);
      
      // Notify gateway that host has changed
      if (this.onHostMigration) {
        this.onHostMigration(lobbyId, nextHost.id);
      }
    }
  }

  private uniqueCode(): string {
    let code: string;
    do {
      code = generateLobbyCode();
    } while (this.codeIndex.has(code));
    return code;
  }

  private scheduleCleanup(lobbyId: string): void {
    if (this.cleanupTimers.has(lobbyId)) return;

    const timer = setTimeout(() => {
      const lobby = this.lobbies.get(lobbyId);
      if (lobby) {
        this.codeIndex.delete(lobby.code);
      }
      this.lobbies.delete(lobbyId);
      this.cleanupTimers.delete(lobbyId);

      // Delete all uploaded files for this lobby
      const lobbyUploadsDir = path.join(process.cwd(), 'uploads', lobbyId);
      fs.rm(lobbyUploadsDir, { recursive: true, force: true }, (err) => {
        if (err) {
          console.error(`[LobbyManager] Failed to delete uploads for lobby ${lobbyId}:`, err);
        } else {
          console.log(`[LobbyManager] Deleted uploads directory for lobby ${lobbyId}`);
        }
      });
    }, LobbyManager.EMPTY_LOBBY_TTL_MS);

    this.cleanupTimers.set(lobbyId, timer);
  }

  private cancelCleanup(lobbyId: string): void {
    const timer = this.cleanupTimers.get(lobbyId);
    if (timer !== undefined) {
      clearTimeout(timer);
      this.cleanupTimers.delete(lobbyId);
    }
  }
}
