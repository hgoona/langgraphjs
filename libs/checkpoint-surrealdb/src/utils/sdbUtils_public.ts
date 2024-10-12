import { debugLog } from "./debugLog.ts";
import { RecordId } from "surrealdb";


/**
         * @param data - unresolved non-POJOs from sdbQuery
         * @returns resolved POJOs
         */
// const FixNonPOJOdata = (data: unknown) => {
// 	const debug_FixNonPOJOdata = true;
// 	// const debug_FixNonPOJOdata = false;

// 	debugLog(debug_FixNonPOJOdata, "FixNonPOJOdata▶▶")
// 	debugLog(debug_FixNonPOJOdata, "unresolved data: ", JSON.stringify(data, null, 2))
// 	try {
// 		const resolvedData = JSON.parse(JSON.stringify(data)) //fix non-POJOs output from sdbQuery
// 		debugLog(debug_FixNonPOJOdata, "resolvedData: ", JSON.stringify(resolvedData, null, 2))

// 		return resolvedData
// 	} catch (e) {
// 		debugLog(debug_FixNonPOJOdata, "FixNonPOJOdata err: ", { data }, JSON.stringify(e, null, 2))
// 		return data //failed to parse or stringify🤷🏾‍♀️
// 	}
// }

export const SerializeNonPOJOs = (data: unknown) => {
    // const debug_SerializeNonPOJOs = true;
    const debug_SerializeNonPOJOs = false;
    debugLog(debug_SerializeNonPOJOs, "SerializeNonPOJOs▶▶")

    if (typeof data === "object" || data === null) {
        debugLog(debug_SerializeNonPOJOs, "unresolved data:", { data })
        debugLog(debug_SerializeNonPOJOs, "unresolved data:", JSON.stringify(data, null, 2))

        const serializedData = structuredClone(data)
        debugLog(debug_SerializeNonPOJOs, "serializedData: ", JSON.stringify(serializedData, null, 2))

        return serializedData
    } else {
        debugLog(debug_SerializeNonPOJOs, "SerializeNonPOJOs▶▶⏭", { data })
        // SKIP PROCESSING
        return data
    }
};


/** 
   * Ensure recordId is a string 
   * eg.
   * - ✅ userId: 'user:h05q3wyaluk4vahoe7jx' 
   * - ❌ userId: RecordId { tb: 'user', id: 'h05q3wyaluk4vahoe7jx' }
   */
export const recordIdToString = (recordId: string | RecordId): string => {
    // const debug_recordIdToString = true;
    const debug_recordIdToString = false;

    debugLog(debug_recordIdToString, "recordIdToString▶▶", recordId)

    if (typeof recordId !== "string") {
        recordId = recordId as RecordId
        return `${recordId.tb}:${recordId.id}`

    } else {
        return recordId as string
    }
}



/**
 * Convert Complex Array RecordId to RecordId for sdb.query bindings
 * 
 * NOTE:❔❔ONLY WORKS for single depth of Array
 * 
 * @param complexId 
 * @returns RecordId
 */
export const ComplexArrayIdToRecordId = (complexId: RecordIdType): RecordId => {
    const { tb, id } = complexId;

    // const debug_on = true;
    const debug_on = false;

    // Extracting the table name and the array of ids
    if (Array.isArray(id)) {
        const idsArray = (id as RecordIdType[]).map(
            item => item
                && typeof item === 'object'
                && item.tb && item.id
                ? new RecordId(item.tb, item.id) // RidId record
                : item // string record

        ); // ONLY WORKS for single depth of Array

        // Constructing the new RecordId instance
        debugLog(debug_on, "complex Array Id");
        return new RecordId(tb, idsArray);
    } else {
        debugLog(debug_on, "simple Id");
        return new RecordId(tb, id);
    }
}