using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericClassificationNode<T, TKey, TClass> : AbstractInputOutputNode<T, T>
        where T : class, INodeOutput<T>, new()
    {
        //private readonly Action<T, TClass> _targetSetter;
        private TKey _indexerKey;
        private readonly Action<T, TKey, TClass> _indexerSetter;
        private readonly Action<T, TClass> _memberSetter;
        private readonly Dictionary<TClass, Func<T, bool>> _classFns;
        private TClass _defaultClass;

        public GenericClassificationNode(Expression<Func<T, TClass>> targetMember)
        {
            _classFns = new Dictionary<TClass, Func<T, bool>>();

            if (TryGetSetter(targetMember, out var memberSetter))
            {
                _memberSetter = memberSetter;
            }
            else if (TryGetIndexerSetter(targetMember, out var indexerSetter))
            {
                _indexerSetter = indexerSetter;
            }

            _defaultClass = default(TClass);
        }

        public GenericClassificationNode<T, TKey, TClass> When(Func<T, bool> predicate, TClass value)
        {
            _classFns[value] = predicate;
            return this;
        }

        public GenericClassificationNode<T, TKey, TClass> Default(TClass value)
        {
            _defaultClass = value;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            foreach (var item in Input)
            {
                var newItem = context.ObjectPool.Borrow<T>();
                try
                {
                    item.CopyTo(newItem);
                    var matchedPredicate = false;

                    foreach (var predicate in _classFns)
                    {
                        if (!predicate.Value(item))
                            continue;

                        SetValue(newItem, predicate.Key);
                        matchedPredicate = true;
                        break;
                    }

                    if (!matchedPredicate)
                        SetValue(newItem, _defaultClass);

                    Emit(newItem);
                    context.ObjectPool.Return(item);
                }
                catch (Exception e)
                {
                    context.ObjectPool.Return(newItem);
                    RaiseError(e, item);
                }
            }

            Emitter.SignalEnd();
        }

        private void SetValue(T item, TClass value)
        {
            if (_memberSetter != null)
                _memberSetter(item, value);
            else if (_indexerSetter != null)
                _indexerSetter(item, _indexerKey, value);
            else
                throw new InvalidOperationException("Could not set value, neither a member or indexer setter are available.");
        }

        private static bool TryGetSetter(Expression<Func<T, TClass>> expression, out Action<T, TClass> setter)
        {
            setter = null;
            if (!(expression.Body is MemberExpression memberExpression))
                return false;

            var property = (PropertyInfo)memberExpression.Member;
            var setMethod = property.GetSetMethod();

            var parameterT = Expression.Parameter(typeof(T), "x");
            var parameterTProperty = Expression.Parameter(typeof(TClass), "y");

            var newExpression =
                Expression.Lambda<Action<T, TClass>>(
                    Expression.Call(parameterT, setMethod, parameterTProperty),
                    parameterT,
                    parameterTProperty
                );

            setter = newExpression.Compile();

            return true;
        }

        private bool TryGetIndexerSetter(Expression<Func<T, TClass>> expression, out Action<T, TKey, TClass> indexerSetter)
        {
            indexerSetter = null;

            if (!(expression.Body is MethodCallExpression methodCallExpression))
                return false;

            if (methodCallExpression.Arguments[0] is MemberExpression)
            {
                var fieldName = ((MemberExpression) methodCallExpression.Arguments[0]).Member.Name;
                var tmp = ((ConstantExpression)((MemberExpression)methodCallExpression.Arguments[0]).Expression).Value;
                _indexerKey = (TKey)tmp.GetType().GetField(fieldName).GetValue(tmp);
            }
            else if (methodCallExpression.Arguments[0] is ConstantExpression)
            {
                _indexerKey = (TKey) ((ConstantExpression) methodCallExpression.Arguments[0]).Value;
            }

            ParameterExpression param = Expression.Parameter(typeof(T), "t");
            ParameterExpression keyExpr = Expression.Parameter(methodCallExpression.Arguments[0].Type);
            ParameterExpression valueExpr = Expression.Parameter(typeof(TClass));
            
            var indexer = param.Type.GetProperty("Item");

            IndexExpression indexExpr = Expression.Property(param, indexer, keyExpr);
            BinaryExpression assign = Expression.Assign(indexExpr, valueExpr);

            var lambdaSetter = Expression.Lambda<Action<T, TKey, TClass>>(assign, param, keyExpr, valueExpr);
            indexerSetter = lambdaSetter.Compile();

            return true;
        }
    }
}