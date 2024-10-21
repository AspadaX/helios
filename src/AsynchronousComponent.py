from typing import (
	Any,
	Coroutine,
	Optional
)

import asyncio
import psutil
from tqdm import tqdm


"""
this component is used to automatically slice tasks into batches
for asynchronous operations. Suited for I/O operations.
"""
class AsynchronousComponent:

	def __init__(self) -> None:
		self.__psutil_process = psutil.Process()

	async def run(
		self,
		tasks: list[Coroutine],
		batch_size: Optional[int] = None
	) -> list[Any]:
		"""
		run the tasks asychronously. if `batch_size`is not specified,
		the method will automatically deterine how big a batch should be.

		Args:
			tasks (list):
			batch_size (Optional[int]):

		Return:
			a list of results from the tasks
		"""
		# determine whether to determine the batch size automatically
		if batch_size == None:
			batch_size = self.__psutil_process.num_threads()

		# slice tasks into batches
		batches: list = [
			[tasks[i:i + batch_size]]
			for i in range(0, len(tasks), batch_size)
		]

		# run the tasks
		all_results: list = []
		for batch in tqdm(batches, desc="Running tasks..."):
			results: list = await asyncio.gather(
				*batch,
				return_exceptions=True
			)
			all_results.extend(results)

		# return the results
		return all_results
