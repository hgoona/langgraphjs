// AUTO CONNECT TO LOCAL SurrealDB
import { execSync, spawn } from "child_process"
import { Surreal } from "surrealdb"

// const path = execSync("which surreal").toString().trim()
// const path = execSync("where surreal").toString().trim()
// const path = "C:\\Users\\Hiranga\\SurrealDB"
const path = "surreal"


console.info(`Using SurrealDB at '${path}'`)

// Start the SurrealDB server at start --bind 127.0.0.1:8001
// spawn(path, ["start", "--bind", "127.0.0.1:8001"])
spawn(path, ["start", "--bind", "127.0.0.1:8001", "--pass", "root", "--user", "root"])

// wait for the database to be ready
while (true) {
	const out = execSync(`${path} is-ready -e http://127.0.0.1:8001`)
	if (out.includes("OK")) {
		console.info("TestSdb - Ready.")
		break
	}
	console.info("TestSdb - Not ready yet. Waiting...")
	await new Promise(resolve => setTimeout(resolve, 50)) // wait 100ms
}

// Now we connect to the database using the SurrealDB SDK
// export const TestSdb = new Surreal() 

let TestSdb: Surreal | null = null;

export const connectSDB = async (): Promise<Surreal> => {
	// console.info("surrealClient:", surrealClient);
	if (!TestSdb) {
		TestSdb = new Surreal()

		await TestSdb.connect("http://127.0.0.1:8001", {
			database: "test",
			namespace: "test",
			auth: {
				username: "root",
				password: "root",
			}
		})

		console.info("TestSdb - Connected")
	} else {
		console.info("TestSdb - ALREADY Connected")
	}
	return TestSdb
}

/**
 * In-Memory SurrealDB database
 * for testing purposes
 */
export const TestSdb1 = await connectSDB()


// Perform some queries or tests
// console.info(await TestSdb1.query("RETURN \"HELLO WORLD\""))

