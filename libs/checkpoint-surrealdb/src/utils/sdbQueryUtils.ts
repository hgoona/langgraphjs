import { debugLog } from "./debugLog.ts";
import { SerializeNonPOJOs } from "./sdbUtils_public.ts";
import { PreparedQuery } from "surrealdb"
import { Surreal } from "surrealdb";

/**
 * SurrealDB Query implementation
 * @example result = await SdbQuery.query( /*surql* / "SELECT * FROM $foo, WHERE bar = $bar", { foo, bar})
 * 
 * 
 * @param query - Surreal QL, a.k.a. "surql query" - SurrealDB's native query language 
 * @example const query = /*surql* / "SELECT * FROM $foo"
 * 
 * @param bindings - List of variable to be used in the query. 
 * @example const bindings = { foo, bar } //where variable `foo` can then be use in query as `$foo`
 * 
 * @param returnArray - BatchedQuery
 * if true, returns queryResults[] - unedited, 
 * ELSE returns singular queryResult from inside of raw output, eg: [[queryResult]]>>>[queryResult]
 * @returns 
 */
export class SdbQueryClass {
	private Sdb: Surreal;
	private debugOn: boolean;

	constructor(Sdb: Surreal, debugOn = true) {
		this.Sdb = Sdb;
		this.debugOn = debugOn;
	}

	async query(
		query: SurrealQuery,
		bindings?: SurrealBindings,
		returnArray?: boolean
	): Promise<unknown | unknown[]> {
		// DEBUG
		// INPUT
		// const debug_query = true && this.debugOn;
		const debug_query = false && this.debugOn;

		// OUTPUT
		// const debug_qData = true && this.debugOn;
		const debug_qData = false && this.debugOn;

		// const debugMode_output = true && this.debugOn;
		const debugMode_output = false && this.debugOn;


		// QUERY REVIEW
		// const queryType = typeof query;
		// console.log({queryType});

		let cleanQuery = query;
		if (typeof query === 'string') {
			cleanQuery = query.trim().replace(/\s+/g, ' '); // clean all
		}

		try {
			debugLog(debug_query, ">>>>>SDB query",
				// "\n", { query },
				"\n", { cleanQuery },
				"\n", { bindings }, // TIGHT
				// "\n", "query:", JSON.stringify(query, null, 2),
				// "\n", "cleanQuery:", JSON.stringify(cleanQuery, null, 2),
				// "\n", "bindings:", JSON.stringify(bindings, null, 2), // VERBOSE
			)
			let qData: unknown[];
			if (query instanceof PreparedQuery) {
				// If query is an instance of PreparedQuery, pass it directly
				qData = await this.Sdb.query(query);
			} else {
				// If query is not an instance of PreparedQuery, treat it as a string and pass it with bindings
				qData = await this.Sdb.query(query, bindings || {});
			}

			debugLog(debug_qData, "SDB>>>>> output",
				"\nQdata: ", JSON.stringify(qData, null, 2)
			)


			// // SDB no longer returns "OK" status👇🏾👇🏾👇🏾
			// // CHECK ERRORS
			// const errors: string[] = [];
			// for (let i = 0; i < qData.length; i++) {
			// 	if (qData[i].status !== "OK") {
			// 		console.log("ERR")
			// 		console.log("ERR:", {query},{cleanQuery}, { bindings})
			// 		errors.push(`ErrDetail${i}: ${qData[i]?.detail}`);
			// 	} else {
			// 		// console.log("OK")
			// 	}
			// }

			// // PACK ERRORS
			// if (errors.length > 0) {
			// 	const e = errors.join("\n")
			// 	console.log("Q_ERR")
			// 	throw new Error(e);
			// } else {
			// 	// console.log("Q_OK")
			// }
			// // SDB no longer returns "OK" status👆🏾👆🏾👆🏾


			// // REVIEW
			// const QresultReview = qData.at(-1).result
			// console.log({QresultReview})
			// Check if qData is not empty
			// if (qData && qData.length !== 0) {
			// 	// throw new Error("No data returned from the query.");
			// 	debugLog(debugMode_output, "QresultArr1: ", qData);

			const hasOnlyNullValues = qData.every(item => item === null || item === undefined);
			if (hasOnlyNullValues) {
				debugLog(debugMode_output, "Query returned NULL!");
				// throw new Error("No non-null values found in the query result.");
				// return qData
				return null
			}


			// ARRAY RESULT	
			if (returnArray) {
				// RETURN ARRAY - no edit
				const resultArr = qData.at(-1) as unknown[];

				debugLog(debugMode_output, "QresultArr: ", resultArr);

				// return resultArr;
				return SerializeNonPOJOs(resultArr);
			} else {
				// PARSE ARRAY - for SDB/Lucia requirements
				if ((qData.at(-1) as unknown[]).length <= 1) {
					const result = (qData.at(-1) as unknown[])[0];
					debugLog(debugMode_output, "Qresult: ", result);

					// return result;
					return SerializeNonPOJOs(result);
				} else { // array greater than 1
					const resultArr = qData.at(-1)
					// debugLog(debugMode_output, "QresultArr2: ", resultArr);
					debugLog(debugMode_output, "QresultArr2: ", JSON.stringify(resultArr, null, 2));

					// return resultArr;
					return SerializeNonPOJOs(resultArr);
				}
			}

		} catch (e) {
			console.log("SdbQueryErr▶▶: ", e);
			// throw new Error((e).message);
			throw new Error((e as Error).message);
		}
	}
}



// const SdbQueryInstance = new SdbQuery.query(Sdb);
// export const SdbQuery = new SdbQueryClass(Sdb);

// // Usage
// const query = /*surql*/ "SELECT * FROM $foo";
// const bindings = { foo: "bar" };
// const result = await SdbQueryInstance.query(query, bindings);
// console.log(result);