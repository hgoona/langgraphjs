/* eslint-disable no-process-env */
import {
	type Checkpoint,
	type CheckpointTuple,
	uuid6,
} from "@langchain/langgraph-checkpoint";

import {
	describe,
	it,
	expect,
	beforeAll,
	type TestOptions,
} from "vitest";
import { TestSdb1 } from "$lib/server/sdbTestDb";
import { SdbQueryClass } from "$lib/server/sdbUtils";
import { SurrealdbSaver } from "./sdbSaver2";


const checkpoint1: Checkpoint = {
	v: 1,
	id: uuid6(-1),
	ts: "2024-04-19T17:19:07.952Z",
	channel_values: {
		someKey1: "someValue1",
	},
	channel_versions: {
		someKey1: 1,
		someKey2: 1,
	},
	versions_seen: {
		someKey3: {
			someKey4: 1,
		},
	},
	pending_sends: [],
};

const checkpoint2: Checkpoint = {
	v: 1,
	id: uuid6(1),
	ts: "2024-04-20T17:19:07.952Z",
	channel_values: {
		someKey1: "someValue2",
	},
	channel_versions: {
		someKey1: 1,
		someKey2: 2,
	},
	versions_seen: {
		someKey3: {
			someKey4: 2,
		},
	},
	pending_sends: [],
};

// const postgresSavers: PostgresSaver[] = [];

describe("Test SDBSaver2", () => {
	/** 
	 * New instance of SurrealdbSaver with (test) DB attached 
	 */
	let SurrealdbSaver: SDBSaver2

	/**
	 * SDBTester is a SdbQueryClass 
	 * that connects to the TestSdb1 database (in-Memory DB).
	 */
	let SDBTester: SdbQueryClass
	beforeAll(async () => {
		SDBTester = new SdbQueryClass(TestSdb1)

		// initialize SurrealdbSaver ❗❗❗❗❗
		SurrealdbSaver = new SurrealdbSaver(SDBTester); // must be an SdbQueryClass
		// SurrealdbSaver = new SurrealdbSaver2(SDBTester); // must be an SdbQueryClass


		const query = /*surql*/ `
			-- init TABLES
			DEFINE TABLE IF NOT EXISTS checkpoints;
			DEFINE TABLE IF NOT EXISTS writes;

			-- define checkpoint fields
			-- =======================
			-- CREATE TABLE IF NOT EXISTS checkpoints (
			-- thread_id TEXT NOT NULL,
			-- checkpoint_ns TEXT NOT NULL DEFAULT '',
			-- checkpoint_id TEXT NOT NULL,
			-- parent_checkpoint_id TEXT,
			-- type TEXT,
			-- checkpoint JSONB NOT NULL,
			-- metadata JSONB NOT NULL DEFAULT '{}',
			-- PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
			-- =======================
			DEFINE FIELD thread_id ON TABLE checkpoints TYPE string; // TEXT NOT NULL, 
			DEFINE FIELD checkpoint_ns ON TABLE checkpoints TYPE string DEFAULT ""; // TEXT NOT NULL DEFAULT '', 
			DEFINE FIELD checkpoint_id ON TABLE checkpoints TYPE string; // TEXT NOT NULL, 
			DEFINE FIELD parent_checkpoint_id ON TABLE checkpoints TYPE option<string>; // TEXT, 
			DEFINE FIELD cpType ON TABLE checkpoints TYPE option<string>; // TEXT, 
			-- DEFINE FIELD checkpoint ON TABLE checkpoints TYPE option<string>; // BLOB, 
			-- DEFINE FIELD metadata ON TABLE checkpoints TYPE option<string>; // BLOB, 


			-- define checkpoint_blobs
			-- =======================
			-- CREATE TABLE IF NOT EXISTS checkpoint_blobs (
			-- thread_id TEXT NOT NULL,
			-- checkpoint_ns TEXT NOT NULL DEFAULT '',
			-- channel TEXT NOT NULL,
			-- version TEXT NOT NULL,
			-- type TEXT NOT NULL,
			-- blob BYTEA,
			-- PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
			-- =======================


			-- define checkpoint_writes
			-- =======================
			-- CREATE TABLE IF NOT EXISTS checkpoint_writes (
			-- thread_id TEXT NOT NULL,
			-- checkpoint_ns TEXT NOT NULL DEFAULT '',
			-- checkpoint_id TEXT NOT NULL,
			-- task_id TEXT NOT NULL,
			-- idx INTEGER NOT NULL,
			-- channel TEXT NOT NULL,
			-- type TEXT,
			-- blob BYTEA NOT NULL,
			-- PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
			-- =======================


			-- review
			INFO FOR TABLE checkpoints;
			INFO FOR TABLE writes;
		`
		const tableInfo = await SDBTester.query(query, {}, true) as Array<unknown>;
		console.log({ tableInfo })
	})


	// it("should save and retrieve checkpoints correctly", async () => {
	// ✅
	it("get undefined checkpoint", async () => {
		// get undefined checkpoint
		const undefinedCheckpoint = await SurrealdbSaver.getTuple({
			configurable: { thread_id: "1" },
		});
		expect(undefinedCheckpoint).toBeUndefined();
	});

	// ✅
	it("save first checkpoint", async () => {
		// save first checkpoint
		const runnableConfig = await SurrealdbSaver.put(
			{ configurable: { thread_id: "1" } },
			checkpoint1,
			{ source: "update", step: -1, writes: null, parents: {} },
			checkpoint1.channel_versions
		);
		expect(runnableConfig).toEqual({
			configurable: {
				thread_id: "1",
				checkpoint_ns: "",
				checkpoint_id: checkpoint1.id,
			},
		});
	});

	// ✅
	it("add some writes", async () => {
		// add some writes
		await SurrealdbSaver.putWrites(
			{
				configurable: {
					checkpoint_id: checkpoint1.id,
					checkpoint_ns: "",
					thread_id: "1",
				},
			},
			[["bar", "baz"]],
			"foo"
		);
	});

	const persistDB = true;
	// const persistDB = false;

	// ❌
	it("get first checkpoint tuple", async () => {
		// get first checkpoint tuple
		console.log("checkpoint1:", JSON.stringify(checkpoint1, null, 2))

		const firstCheckpointTuple = await SurrealdbSaver.getTuple({
			configurable: { thread_id: "1" },
		});
		console.log("firstCheckpointTuple:", JSON.stringify(firstCheckpointTuple, null, 2))
		expect(firstCheckpointTuple?.config).toEqual({
			configurable: {
				thread_id: "1",
				checkpoint_ns: "",
				checkpoint_id: checkpoint1.id,
			},
		});
		expect(firstCheckpointTuple?.checkpoint).toEqual(checkpoint1);
		expect(firstCheckpointTuple?.metadata).toEqual({
			source: "update",
			step: -1,
			writes: null,
			parents: {},
		});
		expect(firstCheckpointTuple?.parentConfig).toBeUndefined();
		expect(firstCheckpointTuple?.pendingWrites).toEqual([
			["foo", "bar", "baz"],
		]);
	});

	// ✅
	it("save second checkpoint", async () => {
		// save second checkpoint
		await SurrealdbSaver.put(
			{
				configurable: {
					thread_id: "1",
					checkpoint_id: "2024-04-18T17:19:07.952Z",
				},
			},
			checkpoint2,
			{ source: "update", step: -1, writes: null, parents: {} },
			checkpoint2.channel_versions
		);
	});

	// ❌
	it("verify that parentTs is set and retrieved correctly for second checkpoint", async () => {
		// verify that parentTs is set and retrieved correctly for second checkpoint
		const secondCheckpointTuple = await SurrealdbSaver.getTuple({
			configurable: { thread_id: "1" },
		});
		expect(secondCheckpointTuple?.metadata).toEqual({
			source: "update",
			step: -1,
			writes: null,
			parents: {},
		});
		expect(secondCheckpointTuple?.parentConfig).toEqual({
			configurable: {
				thread_id: "1",
				checkpoint_ns: "",
				checkpoint_id: "2024-04-18T17:19:07.952Z",
			},
		});
	});

	// ✅
	it("list checkpoints", async () => {
		// list checkpoints
		const checkpointTupleGenerator = SurrealdbSaver.list({
			configurable: { thread_id: "1" },
		});
		const checkpointTuples: CheckpointTuple[] = [];
		for await (const checkpoint of checkpointTupleGenerator) {
			checkpointTuples.push(checkpoint);
		}
		expect(checkpointTuples.length).toBe(2);
		const checkpointTuple1 = checkpointTuples[0];
		const checkpointTuple2 = checkpointTuples[1];
		expect(checkpointTuple1.checkpoint.ts).toBe("2024-04-20T17:19:07.952Z");
		expect(checkpointTuple2.checkpoint.ts).toBe("2024-04-19T17:19:07.952Z");
	});



	// TESTING ================================================================================================
	if (persistDB) {
		//keep the Vitest running to inspect the in-memory DB
		const options: TestOptions = {
			timeout: 2147483647, //max❗❗❗ 24.8 days
		}
		it("KEEP_TEST_DB_ALIVE",
			options,
			async () => {
				await new Promise(resolve => { })
			}
		);
	}
	// TESTING ================================================================================================


});
