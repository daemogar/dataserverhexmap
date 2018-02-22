using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataServer.Utilities;
using Microsoft.AspNetCore.Mvc;

namespace DataServer.Controllers {
	[Produces ("application/json")]
	[Route ("api/{resource}")]
	public class ResourceController : Controller {
		[HttpGet ("~/api/metadata")]
		public async Task<IActionResult> MetaDataAsync () => Json (await Context.TablesAsync ());

		[HttpGet ("~/api/metadatas")]
		public async Task<IActionResult> MetaDatasAsync () => Json ((await Context.TablesAsync ()).SelectMany (p => p.Value.array.Select (q => (fk: q.table, table: p.Key)))
			.ToDictionary (p => p.fk, p => p.table));

		[HttpGet]
		[ProducesResponseType (typeof (List<Object>), 200)]
		public async Task<IActionResult> GetAllAsync ([FromRoute] string resource) => Ok (await new Context (resource).GetAsync ());

		[HttpGet ("{id}", Name = "GetResource")]
		[ProducesResponseType (typeof (Object), 200)]
		[ProducesResponseType (typeof (Object), 400)]
		public async Task<IActionResult> GetOneAsync (long id, [FromRoute] string resource) {
			var context = new Context (resource);
			var list = await context.GetAsync (id);
			var model = list.FirstOrDefault ();

			if (model == null) {
				return BadRequest ($"No {context.Table} with id {id}");
			}

			return Ok (model);
		}

		[HttpPost]
		[ProducesResponseType (typeof (Object), 201)]
		[ProducesResponseType (typeof (Object), 400)]
		public async Task<IActionResult> CreateAsync ([FromBody] Dictionary<string, object> model, [FromRoute] string resource) {
			if (model == null) {
				return BadRequest ("No data provided to create object");
			}

			var (id, data) = await new Context (resource).CreateAsync (model);
			return CreatedAtRoute ("GetResource", new { id }, data);
		}

		[HttpPut ("{id}")]
		[ProducesResponseType (typeof (Object), 202)]
		[ProducesResponseType (typeof (Object), 400)]
		public async Task<IActionResult> UpdateAsync (long id, [FromBody] Dictionary<string, object> model, [FromRoute] string resource) {
			if (model == null) {
				return BadRequest ("No data provided to update object");
			}

			var context = new Context (resource);
			var modelId = context.GetModelValue (model, context.Index);
			if (string.IsNullOrWhiteSpace (modelId) || !long.TryParse (modelId, out var i) || id != i) {
				return BadRequest ($"Model ID does not match [{id}]");
			}

			var data = await context.UpdateAsync (id, model);
			return AcceptedAtRoute ("GetResource", new { id }, data);
		}

		[HttpDelete ("{id}")]
		[ProducesResponseType (typeof (Object), 200)]
		[ProducesResponseType (typeof (Object), 400)]
		public async Task<IActionResult> DeleteAsync (long id, [FromRoute] string resource) => Ok ((await new Context (resource).DeleteAsync (id)) > 0);
	}
}