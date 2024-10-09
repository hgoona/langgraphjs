import { debugLog } from "$lib/debugLog";
import type { SdbQueryClass } from "$lib/server/sdbUtils";
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

/** Option to Serialize or not */
const SERIALIZE = true;
// const SERIALIZE = false;



/**
 * Based on SqliteSaver
 */
export class SurrealdbSaver extends BaseCheckpointSaver {
	public db: SdbQueryClass;

	constructor(db: SdbQueryClass, serde?: SerializerProtocol) {
		super(serde);
		this.db = db;
	}
	// //   constructor(db: DatabaseType, serde?: SerializerProtocol) {
	// //     super(serde);
	// //     this.db = db;
	// //     this.isSetup = false;
	// //   }

	//   static fromConnString(connStringOrLocalPath: string): SqliteSaver {
	//     return new SqliteSaver(new Database(connStringOrLocalPath));
	//   }

	//   protected setup(): void {
	//     if (this.isSetup) {
	//       return;
	//     }

	//     this.db.pragma("journal_mode=WAL");
	//     this.db.exec(`
	// CREATE TABLE IF NOT EXISTS checkpoints (
	//   thread_id TEXT NOT NULL,
	//   checkpoint_ns TEXT NOT NULL DEFAULT '',
	//   checkpoint_id TEXT NOT NULL,
	//   parent_checkpoint_id TEXT,
	//   type TEXT,
	//   checkpoint BLOB,
	//   metadata BLOB,
	//   PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	// );`);
	//     this.db.exec(`
	// CREATE TABLE IF NOT EXISTS writes (
	//   thread_id TEXT NOT NULL,
	//   checkpoint_ns TEXT NOT NULL DEFAULT '',
	//   checkpoint_id TEXT NOT NULL,
	//   task_id TEXT NOT NULL,
	//   idx INTEGER NOT NULL,
	//   channel TEXT NOT NULL,
	//   type TEXT,
	//   value BLOB,
	//   PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
	// );`);

	//     this.isSetup = true;
	//   }


	// NOTE
	//  checkpoints table
	//  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	//  writes table
	//  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)

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

			let rowSDB: CheckpointRow;

			if (checkpoint_id) {
				debugLog(debug_getTuple, "getTuple▶1");

				const bindings = {
					thread_id,
					checkpoint_ns: checkpoint_ns ?? "",
					checkpoint_id
				}

				const query = /*surql*/ `
					SELECT * FROM checkpoints
					WHERE thread_id = $thread_id
					AND checkpoint_ns = $checkpoint_ns
					AND checkpoint_id = $checkpoint_id
				`

				rowSDB = await this.db.query(query, bindings) as CheckpointRow
			} else { // no checkpoint_id
				debugLog(debug_getTuple, "getTuple▶2");

				const bindings = {
					thread_id,
					checkpoint_ns: checkpoint_ns ?? "",
				}

				const query = /*surql*/ `
					SELECT * FROM checkpoints
					WHERE thread_id = $thread_id
					AND checkpoint_ns = $checkpoint_ns
					ORDER BY checkpoint_id DESC 
					LIMIT 1
				`

				rowSDB = await this.db.query(query, bindings) as CheckpointRow
			}

			debugLog(debug_getTuple, "getTuple:", { rowSDB });

			if (!rowSDB) {
				return undefined;
			}

			let finalConfig = config;
			if (!checkpoint_id) {
				debugLog(debug_getTuple, "getTuple▶3");

				finalConfig = {
					configurable: {
						thread_id: rowSDB.thread_id,
						checkpoint_ns: checkpoint_ns ? checkpoint_ns : "", //prevent returning 'undefined'
						checkpoint_id: rowSDB.checkpoint_id,
					},
				};
			}

			if (!finalConfig.configurable?.thread_id || !finalConfig.configurable?.checkpoint_id) {
				debugLog(debug_getTuple, "getTuple▶4");

				throw new Error("Missing thread_id or checkpoint_id");
			}
			debugLog(debug_getTuple, "getTuple▶5", { finalConfig });

			// find any pending writes
			const query = /*surql*/ `
				SELECT * FROM writes:[
					$thread_id,
					$checkpoint_ns,
					$checkpoint_id,
					NONE,
					NONE
				]..
			`

			const bindings = {
				thread_id: finalConfig.configurable.thread_id.toString(),
				checkpoint_ns: checkpoint_ns ?? "",
				checkpoint_id: finalConfig.configurable.checkpoint_id.toString()
			}

			const pendingWritesRowsSDB = await this.db.query(
				query,
				bindings,
				true
			) as WritesRow[]

			debugLog(debug_getTuple, "getTuple▶6a", { pendingWritesRowsSDB });

			const pendingWrites = await Promise.all(
				pendingWritesRowsSDB.map(async (rowSDB: WritesRow) => {
					try {
						const cpValueType = typeof rowSDB.cpValue
						console.log("typeof:", cpValueType)
						console.log("typeof val:", rowSDB.cpValue)

						const cpValueBuffer = (cpValueType == "object")
							? Buffer.from(Object.values(rowSDB.cpValue!).map(Number))
							: rowSDB.cpValue!;

						const cpValue = SERIALIZE
							? await this.serde.loadsTyped(rowSDB.cpType ?? "json", cpValueBuffer)
							: cpValueBuffer

						return [
							rowSDB.task_id,
							rowSDB.channel,
							cpValue
						] as [string, string, unknown];
					} catch (error) {
						console.error("Error in Promise.all callback:", error);
						throw error;
					}
				})
			);

			// SERIALIZE STEP
			// const checkpointBuffer = new Uint8Array(rowSDB.checkpoint as unknown as ArrayBuffer);
			// const metadataBuffer = new Uint8Array(rowSDB.metadata as unknown as ArrayBuffer);
			// const checkpointBuffer = Buffer.from(Object.values(rowSDB.checkpoint as unknown as number[])); // ✅/2
			// const metadataBuffer = Buffer.from(Object.values(rowSDB.metadata as unknown as number[])); // ✅/2

			const checkpointData = SERIALIZE
				? (await this.serde.loadsTyped(
					rowSDB.cpType ?? "json",
					Array.isArray(rowSDB.checkpoint)
						? Buffer.from(Object.values(rowSDB.checkpoint))
						: rowSDB.checkpoint
				)) as Checkpoint
				: (Array.isArray(rowSDB.checkpoint)
					? JSON.parse(JSON.stringify(rowSDB.checkpoint))
					: rowSDB.checkpoint
				);

			const checkpointMetadata = SERIALIZE
				? (await this.serde.loadsTyped(
					rowSDB.cpType ?? "json",
					Array.isArray(rowSDB.metadata)
						? Buffer.from(Object.values(rowSDB.metadata))
						: rowSDB.metadata
				)) as CheckpointMetadata
				: (Array.isArray(rowSDB.metadata)
					? JSON.parse(JSON.stringify(rowSDB.metadata))
					: rowSDB.metadata
				);


			const finalOutput: CheckpointTuple = {
				config: finalConfig,
				checkpoint: checkpointData,
				metadata: checkpointMetadata,
				parentConfig: rowSDB.parent_checkpoint_id
					? {
						configurable: {
							thread_id: rowSDB.thread_id,
							checkpoint_ns: checkpoint_ns ?? "",
							checkpoint_id: rowSDB.parent_checkpoint_id,
						},
					}
					: undefined,
				pendingWrites,
			};

			debugLog(debug_getTuple, "getTuple▶7:", JSON.stringify(finalOutput, null, 2));
			return finalOutput
		} catch (error) {
			console.log("getTuple Err:", error)
		}
	};


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
			const rows = await this.db.query(query, bindings) as CheckpointRow[]


			if (Array.isArray(rows) && rows.length > 0) {
				for (const row of rows) {

					// SERIALIZE STEP
					const checkpointBuffer = Buffer.from(Object.values(row.checkpoint as unknown as number[])); // ✅/2
					const metadataBuffer = Buffer.from(Object.values(row.metadata as unknown as number[])); // ✅/2

					const checkpointData = SERIALIZE
						? (await this.serde.loadsTyped(
							row.cpType ?? "json",
							checkpointBuffer
						)) as Checkpoint
						: (JSON.parse(row.checkpoint) ?? {})

					const checkpointMetadata = SERIALIZE
						? (await this.serde.loadsTyped(
							row.cpType ?? "json",
							metadataBuffer
						)) as CheckpointMetadata
						: (JSON.parse(row.metadata) ?? {})

					yield {
						config: {
							configurable: {
								thread_id: row.thread_id,
								checkpoint_ns: row.checkpoint_ns,
								checkpoint_id: row.checkpoint_id,
							},
						},
						checkpoint: checkpointData,
						metadata: checkpointMetadata,
						parentConfig: row.parent_checkpoint_id
							? {
								configurable: {
									thread_id: row.thread_id,
									checkpoint_ns: row.checkpoint_ns,
									checkpoint_id: row.parent_checkpoint_id,
								},
							}
							: undefined,
					};
				}
			}


		} catch (error) {
			console.log("list Err:", error)
		}
	}


	/**
	 * @note PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
	 * 
	 * @param config 
	 * @param checkpoint 
	 * @param metadata 
	 * @returns 
	 */
	async put(
		config: RunnableConfig,
		checkpoint: Checkpoint,
		metadata: CheckpointMetadata
	): Promise<RunnableConfig> {
		debugLog(debug_checkpointer, `${serviceName} put▶`)

		// const debug_put = true;
		// const debug_put = false;

		try {
			const [type1, serializedCheckpoint] = this.serde.dumpsTyped(checkpoint);
			const [type2, serializedMetadata] = this.serde.dumpsTyped(metadata);
			if (type1 !== type2) {
				throw new Error(
					"Failed to serialized checkpoint and metadata to the same type."
				);
			}

			const checkpointData = SERIALIZE
				? serializedCheckpoint
				: Buffer.from(serializedCheckpoint).toString("utf-8")
			const checkpointMetadata = SERIALIZE
				? serializedMetadata
				: Buffer.from(serializedMetadata).toString("utf-8")

			// const bindings: CheckpointRow = {
			const bindings = {
				thread_id: config.configurable?.thread_id?.toString(),
				checkpoint_ns: config.configurable?.checkpoint_ns ?? "",
				checkpoint_id: checkpoint.id,
				parent_checkpoint_id: config.configurable?.checkpoint_id,
				cpType: type1,
				checkpoint: checkpointData,
				metadata: checkpointMetadata,
			}

			const query = /*surql*/ `
			UPSERT checkpoints:[$thread_id, $checkpoint_ns, $checkpoint_id] CONTENT {
				thread_id: $thread_id,
				checkpoint_ns: $checkpoint_ns,
				checkpoint_id: $checkpoint_id,
				parent_checkpoint_id: $parent_checkpoint_id,
				cpType: $cpType,
				checkpoint: $checkpoint,
				metadata: $metadata
			};
		`

			await this.db.query(query, bindings as unknown as SurrealBindings)

			const configurable = {
				thread_id: config.configurable?.thread_id,
				checkpoint_ns: config.configurable?.checkpoint_ns ?? "",
				checkpoint_id: checkpoint.id,
			}

			return {
				configurable: configurable,
			};

		} catch (error) {
			console.log("put Err:", error)
			throw (error)
		}
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

		try {
			/** get the rows from the writes */
			const rows = writes.map((write, idx) => {
				const [cpType, serializedWrite] = this.serde.dumpsTyped(write[1]);

				console.log("putWrites1:", serializedWrite)
				console.log("putWrites2:", write[1])

				const cpValue = SERIALIZE ? serializedWrite : write[1]

				return {
					thread_id: config.configurable?.thread_id,
					checkpoint_ns: config.configurable?.checkpoint_ns ?? "",
					checkpoint_id: config.configurable?.checkpoint_id,
					task_id: taskId,
					idx: idx,
					channel: write[0],
					cpType: cpType,
					cpValue: cpValue,
				};
			});

			console.log("writes:", writes)
			console.log("rows:", rows)

			// SurrealDB QUERY
			const query = /*surql*/ `
			CREATE writes:[$thread_id, $checkpoint_ns, $checkpoint_id, $task_id, $idx] SET
				thread_id = $thread_id,
				checkpoint_ns = $checkpoint_ns,
				checkpoint_id = $checkpoint_id,
				task_id = $task_id,
				idx = $idx,
				channel = $channel,
				cpType = $cpType,
				cpValue = $cpValue
		`

			for (const row of rows) {
				const { thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, cpType, cpValue } = row
				const bindings = {
					thread_id,
					checkpoint_ns: checkpoint_ns ?? "",
					checkpoint_id,
					task_id,
					idx,
					channel,
					cpType,
					cpValue,
				}
				await this.db.query(query, bindings as unknown as SurrealBindings)
			}

		} catch (error) {
			console.log("putWrites Err:", error)
		}
	}
}