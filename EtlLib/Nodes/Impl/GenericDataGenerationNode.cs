using System;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericDataGenerationNode<TOut, TState> : AbstractOutputNode<TOut>, GenericDataGenerationNode<TOut, TState>.IDataGeneratorHelper<TState>
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly int _fixedCount;
        private readonly Func<EtlPipelineContext, int, IDataGeneratorHelper<TState>, TOut> _generateFn;
        private readonly Func<IDataGeneratorHelper<TState>, bool> _generateFnPredicate;
        private readonly DataGenerationStyle _generationStyle;

        public TState State { get; private set; }
        public TOut LastValue { get; private set; }

        public GenericDataGenerationNode(int numberOfIterations, Func<EtlPipelineContext, int, IDataGeneratorHelper<TState>, TOut> generateFn)
        {
            _fixedCount = numberOfIterations;
            _generateFn = generateFn;
            _generationStyle = DataGenerationStyle.FixedCount;
        }

        public GenericDataGenerationNode(Func<IDataGeneratorHelper<TState>, bool> @while, Func<EtlPipelineContext, int, IDataGeneratorHelper<TState>, TOut> generateFn)
        {
            _generationStyle = DataGenerationStyle.Predicate;
            _generateFn = generateFn;
            _generateFnPredicate = @while;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            switch (_generationStyle)
            {
                case DataGenerationStyle.FixedCount:
                    GenerateUsingFixedCount(context);
                    break;
                case DataGenerationStyle.Predicate:
                    GenerateUsingPredicate(context);
                    break;
            }

            SignalEnd();
        }

        private void GenerateUsingFixedCount(EtlPipelineContext context)
        {
            for (var i = 1; i <= _fixedCount; i++)
            {
                var value = _generateFn(context, i, this);
                LastValue = value;
                Emit(value);
            }
        }

        private void GenerateUsingPredicate(EtlPipelineContext context)
        {
            var count = 0;
            do
            {
                var value = _generateFn(context, ++count, this);
                LastValue = value;
                Emit(value);
            } while (_generateFnPredicate(this));
        }

        public void SetState(TState state)
        {
            State = state;
        }

        private enum DataGenerationStyle
        {
            FixedCount,
            Predicate
        }

        public interface IDataGeneratorHelper<T>
        {
            TOut LastValue { get; }
            T State { get; }
            void SetState(T state);
        }
    }
}