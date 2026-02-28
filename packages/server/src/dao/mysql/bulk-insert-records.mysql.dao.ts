import { HTTPException } from "hono/http-exception";
import type { ResultSetHeader } from "mysql2";
import type { BulkInsertRecordsParams, BulkInsertResult } from "shared/types";
import { getMysqlPool } from "@/db-manager.js";
import { getTableColumns } from "./table-columns.mysql.dao.js";

export const bulkInsertRecords = async ({
	tableName,
	records,
	db,
}: BulkInsertRecordsParams): Promise<BulkInsertResult> => {
	if (!records || records.length === 0) {
		throw new HTTPException(400, {
			message: "At least one record is required",
		});
	}

	const pool = getMysqlPool(db);
	const connection = await pool.getConnection();

	try {
		const columns = Object.keys(records[0]);
		const columnNames = columns.map((col) => `\`${col}\``).join(", ");

		const tableColumns = await getTableColumns({ tableName, db });
		const booleanColumns = new Set(
			tableColumns
				.filter((col) => col.dataTypeLabel === "boolean")
				.map((col) => col.columnName),
		);

		let successCount = 0;
		const errors: Array<{ recordIndex: number; error: string }> = [];

		await connection.beginTransaction();

		for (let i = 0; i < records.length; i++) {
			const record = records[i];
			const values = columns.map((col) => {
				const value = record[col];
				if (booleanColumns.has(col) && typeof value === "string") {
					return value === "true" ? 1 : 0;
				}
				return value;
			});
			const placeholders = columns.map(() => "?").join(", ");

			const insertSQL = `
				INSERT INTO \`${tableName}\` (${columnNames})
				VALUES (${placeholders})
			`;

			try {
				// biome-ignore lint/suspicious/noExplicitAny: mysql2 execute doesn't accept unknown[]
				await connection.execute<ResultSetHeader>(insertSQL, values as any);
				successCount++;
			} catch (error) {
				throw new HTTPException(500, {
					message: `Failed: ${error instanceof Error ? error.message : String(error)}`,
				});
			}
		}

		await connection.commit();

		const failureCount = errors.length;
		return {
			success: failureCount === 0,
			message: `Bulk insert completed: ${successCount} records inserted${failureCount > 0 ? `, ${failureCount} failed` : ""}`,
			successCount,
			failureCount,
			errors: errors.length > 0 ? errors : undefined,
		};
	} catch (error) {
		await connection.rollback();
		if (error instanceof HTTPException) throw error;
		throw new HTTPException(500, {
			message: `Failed to bulk insert records into "${tableName}"`,
		});
	} finally {
		connection.release();
	}
};
