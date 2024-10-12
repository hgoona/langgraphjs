import { debugLog } from "./utils/debugLog.ts";
import type { SdbQueryClass } from "./utils/sdbQueryUtils.ts";

import type { RunnableConfig } from "@langchain/core/runnables";
import {
	BaseCheckpointSaver,
	type Checkpoint,
	type CheckpointListOptions,
	type CheckpointTuple,
	type SerializerProtocol,
	type PendingWrite,
	type CheckpointMetadata,
} from "@langchain/langgraph-checkpoint";

interface CheckpointRow {
	checkpoint: string;
	metadata: string;
	parent_checkpoint_id?: string;
	thread_id: string;
	checkpoint_id: string;
	checkpoint_ns?: string;
	cpType?: string; //fka: 'type'
}

interface WritesRow {
	thread_id: string;
	checkpoint_ns: string;
	checkpoint_id: string;
	task_id: string;
	idx: number;
	channel: string;
	cpType?: string; //fka: 'type'
	cpValue?: string //| ArrayLike<string>; //fka: 'value
}


// DEBUGS
const debug_checkpointer = true
// const debug_checkpointer = false

// const debug_output = true
// // const debug_output = false

const serviceName = "SdbSaver"



/**
 * Based on SqliteSaver
 */
export class SurrealdbSaver extends BaseCheckpointSaver {
	public db: SdbQueryClass;

	constructor(db: SdbQueryClass, serde?: SerializerProtocol) {
		super(serde);
		this.db = db;
	}


	protected async _loadCheckpoint(
		checkpoint: Omit<Checkpoint, "pending_sends" | "channel_values">,
		channelValues: [Uint8Array, Uint8Array, Uint8Array][],
		pendingSends: [Uint8Array, Uint8Array][]
	): Promise<Checkpoint> {
		console.log("_loadCheckpoint_channelValues:", channelValues) // channelValues is missing??

		return {
			...checkpoint,
			pending_sends: await Promise.all(
				(pendingSends || []).map(([c, b]) =>
					this.serde.loadsTyped(c.toString(), b)
				)
			),
			channel_values: await this._loadBlobs(channelValues),
		};
	}

	protected async _loadBlobs(
		blobValues: [Uint8Array, Uint8Array, Uint8Array][]
	): Promise<Record<string, unknown>> {
		if (!blobValues || blobValues.length === 0) {
			return {};
		}
		const entries = await Promise.all(
			blobValues
				.filter(([, t]) => new TextDecoder().decode(t) !== "empty")
				.map(async ([k, t, v]) => [
					new TextDecoder().decode(k),
					await this.serde.loadsTyped(new TextDecoder().decode(t), v),
				])
		);
		return Object.fromEntries(entries);
	}


	protected async _loadMetadata(metadata: Record<string, unknown>) {
		const [type, dumpedValue] = this.serde.dumpsTyped(metadata);
		return this.serde.loadsTyped(type, dumpedValue);
	}

	protected async _loadWrites(
		writes: [Uint8Array, Uint8Array, Uint8Array, Uint8Array][]
	): Promise<[string, string, unknown][]> {
		const decoder = new TextDecoder();
		return writes
			? await Promise.all(
				writes.map(async ([tid, channel, t, v]) => [
					decoder.decode(tid),
					decoder.decode(channel),
					await this.serde.loadsTyped(decoder.decode(t), v),
				])
			)
			: [];
	}

	protected _dumpBlobs(
		threadId: string,
		checkpointNs: string,
		cpValues: Record<string, unknown>, //input channel_values
		cpVersions: ChannelVersions
	): [string, string, string, string, string, Uint8Array | undefined][] {
		if (Object.keys(cpVersions).length === 0) {
			return [];
		}
		console.log("_dumpBlobs_cpValues_INPUT:", JSON.stringify(cpValues, null, 2))

		return Object.entries(cpVersions).map(([k, ver]) => {
			const [cpType, cpValue] =
				k in cpValues ? this.serde.dumpsTyped(cpValues[k]) : ["empty", null];

			console.log("_dumpBlobs_cpValue:", cpValue)
			return [
				threadId,
				checkpointNs,
				k,
				ver.toString(),
				cpType,
				cpValue ? new Uint8Array(cpValue) : undefined,
			];
		});
	}

	protected _dumpCheckpoint(checkpoint: Checkpoint) {
		const serialized: Record<string, unknown> = {
			...checkpoint,
			pending_sends: [],
		};
		if ("channel_values" in serialized) {
			delete serialized.channel_values;
		}
		return serialized;
	}

	protected _dumpMetadata(metadata: CheckpointMetadata) {
		const [, serializedMetadata] = this.serde.dumpsTyped(metadata);
		// We need to remove null characters before writing
		return JSON.parse(
			new TextDecoder().decode(serializedMetadata).replace(/\0/g, "")
		);
	}

	protected _dumpWrites(
		threadId: string,
		checkpointNs: string,
		checkpointId: string,
		taskId: string,
		writes: [string, unknown][]
	): [string, string, string, string, number, string, string, Uint8Array][] {
		return writes.map(([channel, cpValue], idx) => {
			const [cpType, serializedValue] = this.serde.dumpsTyped(cpValue);
			return [
				threadId,
				checkpointNs,
				checkpointId,
				taskId,
				WRITES_IDX_MAP[channel] !== undefined ? WRITES_IDX_MAP[channel] : idx,
				channel,
				cpType,
				new Uint8Array(serializedValue),
			];
		});
	}

	// NOTE
	//  checkpoints table
	//  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	//  writes table
	//  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)

	/**
	 * Get a checkpoint tuple from the database.
	 * This method retrieves a checkpoint tuple from the Postgres database
	 * based on the provided config. If the config's configurable field contains
	 * a "checkpoint_id" key, the checkpoint with the matching thread_id and
	 * namespace is retrieved. Otherwise, the latest checkpoint for the given
	 * thread_id is retrieved.
	 * @param config The config to use for retrieving the checkpoint.
	 * @returns The retrieved checkpoint tuple, or undefined.
	 */
	async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
		debugLog(debug_checkpointer, `${serviceName} getTuple▶`)
		const debug_getTuple = true;
		// const debug_getTuple = false;

		try {
			const {
				thread_id,
				checkpoint_ns,
				checkpoint_id,
			} = config.configurable ?? {};

			debugLog(debug_getTuple, "getTuple:", {
				thread_id,
				checkpoint_ns,
				checkpoint_id
			});


			let query1 = ""
			let bindings1: SurrealBindings = {}
			if (checkpoint_id) {
				debugLog(debug_getTuple, "getTuple▶1");

				bindings1 = {
					thread_id,
					checkpoint_ns: checkpoint_ns ?? "",
					checkpoint_id
				}

				query1 = /*surql*/ `
					SELECT * FROM checkpoints
					WHERE thread_id = $thread_id
					AND checkpoint_ns = $checkpoint_ns
					AND checkpoint_id = $checkpoint_id
				`
			} else { // no checkpoint_id
				debugLog(debug_getTuple, "getTuple▶2");

				bindings1 = {
					thread_id,
					checkpoint_ns: checkpoint_ns ?? "",
				}

				query1 = /*surql*/ `
					SELECT * FROM checkpoints
					WHERE thread_id = $thread_id
					AND checkpoint_ns = $checkpoint_ns
					ORDER BY checkpoint_id DESC 
					LIMIT 1
				`
			}
			const row = await this.db.query(query1, bindings1) //as CheckpointRow

			debugLog(debug_getTuple, "getTuple_row:", JSON.stringify(row, null, 2));

			if (!row) {
				return undefined;
			}

			console.log("getTuple_row.channel_values:", row.channel_values)
			const checkpoint = await this._loadCheckpoint(
				row.checkpoint,
				row.channel_values,
				row.pending_sends
			);
			console.log("getTuple_checkpoint:", checkpoint) //review channel_values ??

			const finalConfig = {
				configurable: {
					thread_id,
					checkpoint_ns: checkpoint_ns ? checkpoint_ns : "",
					checkpoint_id: row.checkpoint_id,
				},
			};

			const metadata = await this._loadMetadata(row.metadata);
			const parentConfig = row.parent_checkpoint_id
				? {
					configurable: {
						thread_id,
						checkpoint_ns,
						checkpoint_id: row.parent_checkpoint_id,
					},
				}
				: undefined;
			const pendingWrites = await this._loadWrites(row.pending_writes);

			return {
				config: finalConfig,
				checkpoint,
				metadata,
				parentConfig,
				pendingWrites,
			};

		} catch (error) {
			console.log("getTuple Err:", error)
		}
	};


	/**
	 * List checkpoints from the database.
	 *
	 * This method retrieves a list of checkpoint tuples from the Postgres database based
	 * on the provided config. The checkpoints are ordered by checkpoint ID in descending order (newest first).
	 */
	async *list(
		config: RunnableConfig,
		options?: CheckpointListOptions
	): AsyncGenerator<CheckpointTuple> {
		const { limit, before } = options ?? {};
		debugLog(debug_checkpointer, `${serviceName} list▶`)

		try {
			const thread_id = config.configurable?.thread_id;

			const bindings = {
				thread_id,
				beforeId: before?.configurable?.checkpoint_id || null
			}
			const query = /*surql*/ `
            SELECT * FROM checkpoints
            WHERE thread_id = $thread_id
            ${before ? `AND checkpoint_id < $beforeId` : ""}
            ORDER BY checkpoint_id DESC
            ${limit ? `LIMIT $limit` : ""}
        `
			const rows = await this.db.query(query, bindings) //as CheckpointRow[]


			if (Array.isArray(rows) && rows.length > 0) {
				for (const row of rows) {
					yield {
						config: {
							configurable: {
								thread_id: row.thread_id,
								checkpoint_ns: row.checkpoint_ns,
								checkpoint_id: row.checkpoint_id,
							},
						},
						checkpoint: await this._loadCheckpoint(
							row.checkpoint,
							row.channel_values,
							row.pending_sends
						),
						metadata: await this._loadMetadata(row.metadata),
						parentConfig: row.parent_checkpoint_id
							? {
								configurable: {
									thread_id: row.thread_id,
									checkpoint_ns: row.checkpoint_ns,
									checkpoint_id: row.parent_checkpoint_id,
								},
							}
							: undefined,
						pendingWrites: await this._loadWrites(row.pending_writes),
					};
				}
			}
		} catch (error) {
			console.log("list Err:", error)
		}
	}


	/**
	 * Save a checkpoint to the database.
	 * @note PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	 *
	 * This method saves a checkpoint to the Postgres database. The checkpoint is associated
	 * with the provided config and its parent config (if any).
	 * @param config
	 * @param checkpoint
	 * @param metadata
	 * @returns
	 */
	async put(
		config: RunnableConfig,
		checkpoint: Checkpoint,
		metadata: CheckpointMetadata,
		newVersions: ChannelVersions
	): Promise<RunnableConfig> {
		debugLog(debug_checkpointer, `${serviceName} put▶`)

		// const debug_put = true;
		// const debug_put = false;

		if (config.configurable === undefined) {
			throw new Error(`Missing "configurable" field in "config" param`);
		}

		const {
			thread_id,
			checkpoint_ns = "",
			checkpoint_id,
		} = config.configurable;

		const nextConfig = {
			configurable: {
				thread_id,
				checkpoint_ns,
				checkpoint_id: checkpoint.id,
			},
		};

		console.log("put_before_checkpoint:", checkpoint)
		console.log("put_before_newVersions:", newVersions)
		const serializedCheckpoint = this._dumpCheckpoint(checkpoint);

		interface CheckpointBlob {
			thread_id: string,
			checkpoint_ns: string,
			channel: string,
			cpVersion: string,
			cpType: string,
			blob: Uint8Array | undefined
		}
		const rawSerializedBlobs = this._dumpBlobs(
			thread_id,
			checkpoint_ns,
			checkpoint.channel_values,
			newVersions
		);
		console.log("put_rawSerializedBlobs:", JSON.stringify(rawSerializedBlobs, null, 2))
		const serializedBlobs: CheckpointBlob[] = rawSerializedBlobs.map((blob) => ({
			thread_id: blob[0],
			checkpoint_ns: blob[1],
			channel: blob[2],
			cpVersion: blob[3],
			cpType: blob[4],
			blob: blob[5]
		}))
		console.log("put_serializedBlobs:", JSON.stringify(serializedBlobs, null, 2))

		const queryPutTX = /*surql*/ `
			BEGIN TRANSACTION;
			
			// UPSERT_CHECKPOINT_BLOBS_SDB
			FOR $serializedBlob IN $serializedBlobs {
				UPSERT checkpoint_blobs:[
						$serializedBlob.thread_id, 
						$serializedBlob.checkpoint_ns,
						$serializedBlob.channel,
						$serializedBlob.cpVersion
					] 
				CONTENT {
					thread_id: $serializedBlob.thread_id, 
					checkpoint_ns: $serializedBlob.checkpoint_ns,
					channel: $serializedBlob.channel,
					cpVersion: $serializedBlob.cpVersion,
					cpType: $serializedBlob.cpType, 
					blob: $serializedBlob.blob
				} 
			}
			;

			-- UPSERT_CHECKPOINTS_SDB
			INSERT INTO checkpoints {
				id:[
					$thread_id,
					$checkpoint_ns,
					$checkpoint_id
				],
				thread_id: $thread_id,
				checkpoint_ns: $checkpoint_ns,
				checkpoint_id: $checkpoint_id,
				parent_checkpoint_id: $parent_checkpoint_id,
				checkpoint: $checkpoint,
				metadata: $metadata
			}
			ON DUPLICATE KEY UPDATE
				checkpoint = $checkpoint,
				metadata = $metadata
			;

			COMMIT TRANSACTION;
		`
		const bindingsPut = {
			serializedBlobs, // Array - data for serialized blob
			thread_id,
			checkpoint_ns,
			checkpoint_id: checkpoint.id,
			parent_checkpoint_id: checkpoint_id,
			checkpoint: serializedCheckpoint,
			metadata: this._dumpMetadata(metadata),
		}

		await this.db.query(queryPutTX, bindingsPut);

		return nextConfig
	}


	/**
	 * @note PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
	 * 
	 * @param config 
	 * @param writes 
	 * @param taskId 
	 */
	async putWrites(
		config: RunnableConfig,
		writes: PendingWrite[],
		taskId: string
	): Promise<void> {
		debugLog(debug_checkpointer, `${serviceName} putWrites▶`)

		const overwrite = writes.every((w) => w[0] in WRITES_IDX_MAP);

		const rawDumpedWrites = this._dumpWrites(
			config.configurable?.thread_id,
			config.configurable?.checkpoint_ns,
			config.configurable?.checkpoint_id,
			taskId,
			writes
		);

		/** Array. Transformed for SDB bindings */
		const dumpedWrites: WritesRow[] = rawDumpedWrites.map((write) => ({
			thread_id: write[0],
			checkpoint_ns: write[1],
			checkpoint_id: write[2],
			task_id: write[3],
			idx: write[4],
			channel: write[5],
			cpType: write[6],
			blob: write[7],
		}));

		const queryPutWritesTX = /*surql*/ `
			BEGIN TRANSACTION;
			FOR $dumpedWrite IN $dumpedWrites {
				INSERT INTO checkpoint_writes {
					id:[
						$dumpedWrite.thread_id,
						$dumpedWrite.checkpoint_ns,
						$dumpedWrite.checkpoint_id,
						$dumpedWrite.task_id,
						$dumpedWrite.idx
					],
					thread_id: $dumpedWrite.thread_id,
					checkpoint_ns: $dumpedWrite.checkpoint_ns,
					checkpoint_id: $dumpedWrite.checkpoint_id,
					task_id: $dumpedWrite.task_id,
					idx: $dumpedWrite.idx,
					channel: $dumpedWrite.channel,
					cpType: $dumpedWrite.cpType,
					blob: $dumpedWrite.blob
				}
				${overwrite ?
				/*surql*/ `
					ON DUPLICATE KEY UPDATE
					channel = $dumpedWrite.channel,
					cpType = $dumpedWrite.cpType,
					blob = $dumpedWrite.blob
				`
				: ""}
			};
			COMMIT TRANSACTION;
		`

		await this.db.query(queryPutWritesTX, { dumpedWrites })
	}
}