using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EtlLib.Pipeline.Operations
{
    public class ParallelOperation : AbstractEtlPipelineOperation
    {
        private readonly List<IEtlPipelineOperation> _steps;

        public ParallelOperation(string name, params IEtlPipelineOperation[] executables)
        {
            _steps = new List<IEtlPipelineOperation>(executables);
            SetName(name);
        }
        
        public override IEtlPipelineOperationResult Execute()
        {
            var tasks = new ConcurrentBag<Task>();
            var errors = new ConcurrentBag<EtlPipelineOperationError>();
            var isSuccess = true;

            foreach (var step in _steps)
            {
                var task = Task.Run(() =>
                {
                    var result = step.Execute();
                    foreach (var err in result.Errors)
                    {
                        errors.Add(err);
                    }

                    if (!result.IsSuccess)
                        isSuccess = false;
                });
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());

            return new EtlPipelineOperationResult(isSuccess);
        }
    }
}