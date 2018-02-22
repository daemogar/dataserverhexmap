using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataServer.Utilities {
	public class Context {
		private static SqlConnectionStringBuilder Builder = new SqlConnectionStringBuilder {
			DataSource = "hexmap.database.windows.net",
				UserID = "hexmap",
				Password = "Hmap11!`hex",
				InitialCatalog = "hexmap"
		};

		private static Dictionary<string, (string pk, (string table, string fk) [] array)> _tables;
		public static async Task<Dictionary<string, (string pk, (string table, string fk) [] array)>> TablesAsync () {
			if (_tables == null) {
				_tables = new Dictionary<string, (string pk, (string table, string fk) [] array)> ();

				var tables = (await Context.QueryAsync ($"Select [TABLE_NAME] From [{Builder.InitialCatalog}].[INFORMATION_SCHEMA].[TABLES] Where [TABLE_SCHEMA] = 'dbo' ")).Select (p => p[0]).ToList ();
				foreach (var table in tables) {
					var columns = (await Context.QueryAsync ($"Select [COLUMN_NAME] From [INFORMATION_SCHEMA].[COLUMNS] Where [table_name] = '{table}' "))
						.Where (p => p[0].ToString ().EndsWith ("ID") && !table.Equals (p[0].ToString ().Replace ("ID", "") + "s"))
						.Select (p => (p[0].ToString (), p[0].ToString ().Replace ("ID", "") + "s"))
						.ToArray ();

					var reference = table.ToString ();
					_tables.Add (reference.ToLower (), (reference.TrimEnd ('s') + "ID", columns));
				}
			}

			return _tables;
		}

		public string Table { get; private set; }
		public string Index { get; private set; }

		public Context (string table) {
			this.Table = table;
			this.Index = table[0].ToString ().ToUpper () + table.Substring (1).TrimEnd ('s') + "ID";
		}

		public static async Task<List<List<object>> > QueryAsync (string query) {
			using (SqlConnection connection = new SqlConnection (Builder.ConnectionString)) {
				using (SqlCommand command = new SqlCommand (query, connection)) {
					await connection.OpenAsync ();

					using (SqlDataReader reader = await command.ExecuteReaderAsync ()) {
						var list = new List<List<object>> ();

						while (await reader.ReadAsync ()) {
							var items = new List<object> ();

							for (var i = 0; i < reader.FieldCount; i++) {
								items.Add (reader.GetValue (i));
							}

							list.Add (items);
						}

						return list;
					}
				}
			}
		}

		public async Task<List<string>> MetaAsync () {
			using (SqlConnection connection = new SqlConnection (Builder.ConnectionString)) {
				using (SqlCommand command = new SqlCommand ($"Select Top 0 * From [{Table}]", connection)) {
					await connection.OpenAsync ();

					using (SqlDataReader reader = command.ExecuteReader ()) {
						var fields = new List<string> ();
						for (var i = 0; i < reader.FieldCount; i++) {
							var name = reader.GetName (i);

							if (name.ToLower ().Equals (Index.ToLower ()))
								continue;

							fields.Add (name);
						}

						return fields;
					}
				}
			}
		}

		private string SelectOrDelete (string type, long id) {
			StringBuilder sb = new StringBuilder ();
			sb.Append ($"{type} From [{Table}] ");

			if (id > 0L) {
				sb.Append ($"Where [{Table}].[{Index}] = '{id}' ");
			}

			return sb.ToString ();
		}

		private List<string> IgnoreDeepTables = new List<string> ();

		public async Task<List<Dictionary<string, object>> > GetAsync (long id = 0) {

			using (SqlConnection connection = new SqlConnection (Builder.ConnectionString)) {

				await connection.OpenAsync ();

				var list = new List<Dictionary<string, object>> ();

				using (SqlCommand command = new SqlCommand (SelectOrDelete ("Select *", id), connection)) {
					using (SqlDataReader reader = await command.ExecuteReaderAsync ()) {
						var count = reader.FieldCount;
						var values = new Object[reader.FieldCount];

						var tables = await TablesAsync ();
						var linkedArray = tables.SelectMany (p => p.Value.array.Select (q => (fk: q.table, table: p.Key)))
							.ToDictionary (p => p.fk, p => p.table);

						while (reader.Read ()) {
							reader.GetValues (values);

							var model = new Dictionary<string, object> ();
							for (var i = 0; i < count; i++) {
								var field = reader.GetName (i);
								model.Add (field, values[i]);

								if (!IgnoreDeepTables.Contains (Table) && linkedArray.ContainsKey (field) && !linkedArray[field].Equals (Table)) {
									var context = new Context (linkedArray[field]);
									context.IgnoreDeepTables.Add (Table);

									var data = (await context.GetAsync ()).Where (p => p[field].Equals (model[field])).ToList ();
									model.Add (context.Table, data);
								}
							}

							var nestedObjects = tables[Table.ToLower ()].array;
							foreach (var (fk, table) in nestedObjects) {
								var context = new Context (table);
								context.IgnoreDeepTables.Add (table);

								var data = (await context.GetAsync (long.Parse (model[fk].ToString ()))).FirstOrDefault ();
								model.Add (table.TrimEnd ('s'), data);
							}

							list.Add (model);
						}
					}
				}

				return list;
			}
		}

		public async Task<Dictionary<string, object>> UpdateAsync (long id, Dictionary<string, object> model) {
			return (await CreateOrUpdateAsync ("Update", id, model)).data;
		}

		public async Task<(long id, Dictionary<string, object>)> CreateAsync (Dictionary<string, object> model) {
			return await CreateOrUpdateAsync ("Insert Into", 0, model);
		}

		private bool IsNumeric (object data) => long.TryParse (data.ToString (), out var number);

		public string GetModelValue (Dictionary<string, object> model, string key) {
			var data = model.FirstOrDefault (p => p.Key.Equals (key, StringComparison.InvariantCultureIgnoreCase)).Value;
			return data == null ? "NULL" : IsNumeric (data) ? data.ToString () : $"'{data}'";
		}

		private async Task<(long id, Dictionary<string, object> data)> CreateOrUpdateAsync (string type, long id, Dictionary<string, object> model) {
			using (SqlConnection connection = new SqlConnection (Builder.ConnectionString)) {
				var query = await UpdateOrCreateQueryAsync (type, id, model);

				connection.Open ();

				using (SqlCommand command = new SqlCommand (query, connection)) {
					if (id == 0) {
						model[Index] = await command.ExecuteScalarAsync ();
					} else {
						await command.ExecuteNonQueryAsync ();
					}

					return (Convert.ToInt64 (GetModelValue (model, Index)), model);
				}
			}
		}

		private async Task<string> UpdateOrCreateQueryAsync (string type, long id, Dictionary<string, object> model) {
			var fields = await MetaAsync ();

			StringBuilder sb = new StringBuilder ();
			sb.Append ($"{type} [{Table}] ");

			if (id > 0) {
				sb.Append ("Set " + string.Join (", ", fields.Select (p => $"[{Table}].[{p}] = {GetModelValue(model, p)}")));
				sb.Append ($" Where [{Table}].[{Index}] = {id} ");
			} else {
				sb.Append ($"([{Table}].[");
				sb.Append (string.Join ($"], [{Table}].[", fields));
				sb.Append ("]) Values (");
				sb.Append (string.Join (", ", fields.Select (p => GetModelValue (model, p))));
				sb.Append ("); Select SCOPE_IDENTITY(); ");
			}

			return sb.ToString ();
		}

		public async Task<int> DeleteAsync (long id) {
			using (SqlConnection connection = new SqlConnection (Builder.ConnectionString)) {
				connection.Open ();

				var query = SelectOrDelete ("Delete", id) + "; Select @@ROWCOUNT; ";
				using (SqlCommand command = new SqlCommand (query, connection)) {
					return int.Parse ((await command.ExecuteScalarAsync ()).ToString ());
				}
			}
		}
	}
}