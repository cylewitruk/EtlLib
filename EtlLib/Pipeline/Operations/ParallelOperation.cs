using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EtlLib.Pipeline.Operations
{
    public class ParallelOperation : AbstractEtlOperationWithNoResult
    {
        private readonly List<IEtlOperation> _steps;

        public ParallelOperation(string name, params IEtlOperation[] executables)
        {
            _steps = new List<IEtlOperation>(executables);
            Named(name);
        }
        
        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            var tasks = new ConcurrentBag<Task>();
            var errors = new ConcurrentBag<EtlOperationError>();
            var isSuccess = true;

            foreach (var step in _steps)
            {
                var task = Task.Run(() =>
                {
                    var result = step.Execute(context);
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

            return new EtlOperationResult(isSuccess);
        }
    }
}