using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using DynamicsXchangeToolkit.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace DynamicsXchangeToolkit;

// GenericCrmService<T> is a generic service for CRUD and navigation operations on Microsoft Dataverse (CRM) entities.
// T must be a POCO class decorated with [CrmTable] and [CrmColumn]/[CrmForeignKey] attributes for mapping.
public class GenericCrmService<T> : IGenericCrmService<T> where T : class, new()
{
    // The Dataverse ServiceClient used for all CRM operations.
    private readonly ServiceClient _serviceClient;

    // The logical table name in Dataverse, determined from the CrmTable attribute.
    private readonly string _tableName;

    // Constructor: Validates T has a CrmTable attribute and sets up the table name.
    // Throws InvalidOperationException if T is not properly decorated.
    public GenericCrmService(ServiceClient serviceClient)
    {
        _serviceClient = serviceClient;
        var tableAttr = typeof(T).GetCustomAttribute<CrmTableAttribute>();
        if (tableAttr == null)
            throw new InvalidOperationException($"Class {typeof(T).Name} must have a CrmTable attribute.");
        _tableName = tableAttr.TableName;
    }

    /// <summary>
    /// Creates a new record in Dataverse for the given entity.
    /// Converts the POCO to a CRM Entity using reflection and attributes, then calls Dataverse to create the record asynchronously.
    /// Returns the Guid of the created record; throws when Dataverse rejects the operation.
    /// </summary>
    public async Task<Guid?> CreateAsync(T entity)
    {
        try
        {
            // Convert the POCO to a CRM Entity using reflection and attributes.
            var crmEntity = ToCrmEntity(entity);
            // Call Dataverse to create the record asynchronously.
            return await _serviceClient.CreateAsync(crmEntity);
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to create {typeof(T).Name} record.", ex);
        }
    }

    /// <summary>
    /// Retrieves a record by its Guid primary key from Dataverse.
    /// Returns the mapped POCO instance.
    /// </summary>
    public async Task<T> RetrieveAsync(Guid id)
    {
        try
        {
            // Get all CRM columns for T using attribute mapping.
            var columns = GetColumns();
            // Retrieve the CRM entity by id.
            var crmEntity = await _serviceClient.RetrieveAsync(_tableName, id, new Microsoft.Xrm.Sdk.Query.ColumnSet(columns));
            // Convert the CRM entity to a POCO.
            return ToPOCO(crmEntity);
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} record with id '{id}'.", ex);
        }
    }

    /// <summary>
    /// Retrieves the first record matching a predicate (in-memory filter).
    /// Loads all records and applies the predicate in .NET (not in CRM).
    /// </summary>
    public async Task<T?> RetrieveAsync(Expression<Func<T, bool>> predicate)
    {

        try
        {
            var columns = GetColumns();
            var query = new Microsoft.Xrm.Sdk.Query.QueryExpression(_tableName)
            {
                ColumnSet = new Microsoft.Xrm.Sdk.Query.ColumnSet(columns)
            };

            BuildQueryFilter(query, predicate);

            var result = await _serviceClient.RetrieveMultipleAsync(query);
            return result.Entities.Select(ToPOCO).FirstOrDefault();
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} record by predicate.", ex);
        }
    }

    /// <summary>
    /// Updates a record by its Guid primary key.
    /// Converts POCO to CRM entity and sets the id, then calls Dataverse to update the record.
    /// Returns true when the update succeeds; throws when Dataverse rejects the operation.
    /// </summary>
    public async Task<bool> UpdateAsync(Guid id, T entity)
    {
        try
        {
            // Convert POCO to CRM entity and set the id.
            var crmEntity = ToCrmEntity(entity);
            crmEntity.Id = id;
            // Call Dataverse to update the record.
            await _serviceClient.UpdateAsync(crmEntity);
            return true;
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to update {typeof(T).Name} record with id '{id}'.", ex);
        }
    }

    /// <summary>
    /// Deletes a record by its Guid primary key.
    /// Throws when Dataverse rejects the delete operation.
    /// </summary>
    public async Task DeleteAsync(Guid id)
    {
        try
        {
            await _serviceClient.DeleteAsync(_tableName, id);
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to delete {typeof(T).Name} record with id '{id}'.", ex);
        }
    }

    /// <summary>
    /// Retrieves all records for T, optionally paginated.
    /// Returns a list of POCOs. Throws on error.
    /// </summary>
    public async Task<List<T>> RetrieveAllAsync(Pagination? pagination = null)
    {
        try
        {
            var columns = GetColumns();
            var query = new Microsoft.Xrm.Sdk.Query.QueryExpression(_tableName)
            {
                ColumnSet = new Microsoft.Xrm.Sdk.Query.ColumnSet(columns),
            };
            if (pagination != null && pagination.PageNumber.HasValue && pagination.PageSize.HasValue)
            {
                query.PageInfo = new Microsoft.Xrm.Sdk.Query.PagingInfo
                {
                    PageNumber = pagination.PageNumber.Value,
                    Count = pagination.PageSize.Value,
                    PagingCookie = null // Not required for basic paging
                };
            }
            var result = await _serviceClient.RetrieveMultipleAsync(query);
            return result.Entities.Select(ToPOCO).ToList();
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} records.", ex);
        }
    }

    /// <summary>
    /// Retrieves all records matching a predicate (in-memory filter), optionally paginated.
    /// Loads all records from CRM, then filters in .NET.
    /// </summary>
    public async Task<List<T>> RetrieveAllAsync(Expression<Func<T, bool>> predicate, Pagination? pagination = null)
    {
        try
        {
            var columns = GetColumns();
            var query = new Microsoft.Xrm.Sdk.Query.QueryExpression(_tableName)
            {
                ColumnSet = new Microsoft.Xrm.Sdk.Query.ColumnSet(columns)
            };

            BuildQueryFilter(query, predicate);
            BuildQueryPagination(query, pagination);

            var result = await _serviceClient.RetrieveMultipleAsync(query);
            return result.Entities.Select(ToPOCO).ToList();
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} records by predicate.", ex);
        }
    }









    #region --- Navigation loading methods ---

    /// <summary>
    /// Retrieves a record by id and loads its navigation properties in batch.
    /// Uses reflection and service provider to recursively load related entities.
    /// </summary>
    public async Task<T?> RetrieveWithNavigationBatchByIdAsync(Guid id, IServiceProvider sp)
    {
        try
        {
            var entity = await RetrieveAsync(id);
            if (entity == null) return null;
            var list = new List<T> { entity };
            // Recursively fill navigation properties for the entity
            await FillNavigationPropertiesBatchAsync(list, sp, new HashSet<Type>());
            return entity;
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} navigation graph by id '{id}'.", ex);
        }
    }

    /// <summary>
    /// Retrieves all records and loads navigation properties in batch.
    /// Loads all related entities recursively for each navigation property.
    /// </summary>
    public async Task<List<T>> RetrieveAllWithNavigationBatchAsync(IServiceProvider sp, Pagination? pagination = null)
    {
        try
        {
            var mainList = await RetrieveAllAsync(pagination);
            // Recursively fill navigation properties for all entities
            await FillNavigationPropertiesBatchAsync(mainList, sp, new HashSet<Type>());
            return mainList;
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} navigation graph collection.", ex);
        }
    }



    /// <summary>
    /// Retrieves records by a list of Guids (primary keys).
    /// Used for batch loading navigation properties.
    /// </summary>
    public async Task<List<T>> RetrieveByIdsAsync(IEnumerable<Guid> ids)
    {
        try
        {
            var columns = GetColumns();
            var pk = ResolvePrimaryKeyLogicalName(typeof(T));
            var query = new QueryExpression(_tableName) { ColumnSet = new ColumnSet(columns) };
            query.Criteria.AddCondition(pk, ConditionOperator.In, ids.Select(x => (object)x).ToArray());
            var result = await _serviceClient.RetrieveMultipleAsync(query);
            return result.Entities.Select(ToPOCO).ToList();
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} records by identifiers.", ex);
        }
    }

    /// <summary>
    /// Retrieves child records by parent key (for collection navigation properties).
    /// Used for 1-to-many navigation loading.
    /// </summary>
    public async Task<List<T>> RetrieveByParentKeyAsync(PropertyInfo childFkProp, IEnumerable<object?> parentKeys)
    {
        try
        {
            var columns = GetColumns();
            var fkCol = childFkProp.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName
                        ?? throw new InvalidOperationException("FK property missing [CrmColumn].");
            var query = new QueryExpression(_tableName) { ColumnSet = new ColumnSet(columns) };
            query.Criteria.AddCondition(fkCol, ConditionOperator.In, parentKeys.Where(x => x != null).ToArray());
            var result = await _serviceClient.RetrieveMultipleAsync(query);
            return result.Entities.Select(ToPOCO).ToList();
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve child {typeof(T).Name} records via foreign key '{childFkProp.Name}'.", ex);
        }
    }

    /// <summary>
    /// Retrieves a record by predicate and loads navigation properties in batch.
    /// Loads all records, then finds the first matching and loads its navigation.
    /// </summary>
    public async Task<T?> RetrieveWithNavigationBatchByPredicateAsync(
        IServiceProvider sp,
        Expression<Func<T, bool>> predicate)
    {
        try
        {
            var query = BuildQueryWithJoins(predicate, limitOne: true);
            var page = await _serviceClient.RetrieveMultipleAsync(query);
            var list = page.Entities.Select(ToPOCO).ToList();
            if (list.Count == 0) return null;

            await FillNavigationPropertiesBatchAsync(list, sp, new HashSet<Type>());
            return list.First();
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} navigation graph by predicate.", ex);
        }
    }


    /// <summary>
    /// Retrieves all records matching a predicate and loads navigation properties in batch.
    /// </summary>
    public async Task<List<T>> RetrieveAllWithNavigationBatchAsync(
        IServiceProvider sp,
        Expression<Func<T, bool>> predicate,
        Pagination? pagination = null)
    {
        try
        {
            var query = BuildQueryWithJoins(predicate, limitOne: false);
            ApplyPagination(query, pagination);

            var result = await _serviceClient.RetrieveMultipleAsync(query);
            var list = result.Entities.Select(ToPOCO).ToList();

            await FillNavigationPropertiesBatchAsync(list, sp, new HashSet<Type>());
            return list;
        }
        catch (Exception ex)
        {
            throw new GenericCrmServiceException($"Failed to retrieve {typeof(T).Name} navigation graph collection by predicate.", ex);
        }
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Converts a POCO to a CRM Entity for Dataverse operations.
    /// Uses [CrmColumn] attributes to map properties to CRM logical names.
    /// </summary>
    private Entity ToCrmEntity(T poco)
    {
        var entity = new Entity(_tableName);
        foreach (var prop in typeof(T).GetProperties())
        {
            var attr = prop.GetCustomAttribute<CrmColumnAttribute>();
            if (attr != null)
            {
                var value = prop.GetValue(poco);
                if (value != null)
                {
                    // If the property is an EntityReference, assign directly
                    if (value is EntityReference er)
                        entity[attr.LogicalName] = er;
                    else
                        entity[attr.LogicalName] = value;
                }
            }
        }
        return entity;
    }


    /// <summary>
    /// Gets the logical name of the primary key column for T using [CrmTable] attribute.
    /// </summary>
    private string GetPrimaryKeyLogicalName(Type? t = null)
    {
        t ??= typeof(T);
        var tableAttr = t.GetCustomAttribute<CrmTableAttribute>();
        if (tableAttr == null) throw new InvalidOperationException("Missing CrmTable attribute.");
        return tableAttr.TableName + "id";
    }


    // --- Expression to CRM Filter Parser ---

    private object ExpressionToFilter(Expression expr)
    {
        if (expr is BinaryExpression be)
        {
            // AND / OR
            if (be.NodeType == ExpressionType.AndAlso || be.NodeType == ExpressionType.OrElse)
            {
                var filter = new FilterExpression(
                    be.NodeType == ExpressionType.AndAlso
                        ? LogicalOperator.And
                        : LogicalOperator.Or);

                var left = ExpressionToFilter(be.Left);
                var right = ExpressionToFilter(be.Right);

                if (left is ConditionExpression lc)
                    filter.AddCondition(lc);
                else if (left is FilterExpression lf)
                    filter.AddFilter(lf);

                if (right is ConditionExpression rc)
                    filter.AddCondition(rc);
                else if (right is FilterExpression rf)
                    filter.AddFilter(rf);

                return filter;
            }

            // Comparison
            var propExpr = be.Left as MemberExpression ?? be.Right as MemberExpression;
            var valExpr = be.Left == propExpr ? be.Right : be.Left;

            // Null checks
            var isLeftNull = be.Left is ConstantExpression lce && lce.Value == null;
            var isRightNull = be.Right is ConstantExpression rce && rce.Value == null;
            bool isNullCheck = isLeftNull || isRightNull;

            if (propExpr is MemberExpression member && member.Member is PropertyInfo propInfo)
            {
                var crmCol = propInfo.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                if (crmCol == null)
                    throw new InvalidOperationException($"Property {propInfo.Name} missing CrmColumnAttribute.");
                object? value = null;
                if (!isNullCheck)
                {
                    value = Expression.Lambda(valExpr).Compile().DynamicInvoke();
                    if (value == null)
                        throw new InvalidOperationException("Comparison value evaluated to null. Use explicit null checks instead.");
                }

                switch (be.NodeType)
                {
                    case ExpressionType.Equal:
                        if (isNullCheck)
                            return new ConditionExpression(crmCol, ConditionOperator.Null);
                        return new ConditionExpression(crmCol, ConditionOperator.Equal, value);
                    case ExpressionType.NotEqual:
                        if (isNullCheck)
                            return new ConditionExpression(crmCol, ConditionOperator.NotNull);
                        return new ConditionExpression(crmCol, ConditionOperator.NotEqual, value);
                    case ExpressionType.GreaterThan:
                        return new ConditionExpression(crmCol, ConditionOperator.GreaterThan, value);
                    case ExpressionType.GreaterThanOrEqual:
                        return new ConditionExpression(crmCol, ConditionOperator.GreaterEqual, value);
                    case ExpressionType.LessThan:
                        return new ConditionExpression(crmCol, ConditionOperator.LessThan, value);
                    case ExpressionType.LessThanOrEqual:
                        return new ConditionExpression(crmCol, ConditionOperator.LessEqual, value);
                }
            }
            throw new NotSupportedException("Only simple member comparisons are supported.");
        }
        else if (expr is MethodCallExpression mce && mce.Method.Name == "Contains")
        {
            // IN: collection.Contains(property)
            if (mce.Arguments.Count == 1 && mce.Arguments[0] is MemberExpression argProp && argProp.Member is PropertyInfo propInfo)
            {
                // If the collection is a string, treat as LIKE, else as IN
                var collectionType = mce.Object?.Type ?? mce.Method.DeclaringType;
                if (collectionType == typeof(string))
                {
                    // LIKE
                    var crmCol = propInfo.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                    if (crmCol == null)
                        throw new InvalidOperationException($"Property {propInfo.Name} missing CrmColumnAttribute.");
                    var value = Expression.Lambda(mce.Arguments[0]).Compile().DynamicInvoke()?.ToString();
                    return new ConditionExpression(crmCol, ConditionOperator.Like, $"%{value}%");
                }
                else
                {
                    object? collectionValue = null;
                    if (mce.Object != null)
                        collectionValue = Expression.Lambda(mce.Object).Compile().DynamicInvoke();
                    else if (mce.Arguments.Count == 2)
                        collectionValue = Expression.Lambda(mce.Arguments[1]).Compile().DynamicInvoke();

                    var crmCol = propInfo.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                    if (crmCol == null)
                        throw new InvalidOperationException($"Property {propInfo.Name} missing CrmColumnAttribute.");

                    var inValues = (collectionValue as IEnumerable<object>)
                        ?? (collectionValue as IEnumerable<string>)?.Cast<object>()
                        ?? (collectionValue as IEnumerable<Guid>)?.Cast<object>();
                    if (inValues == null)
                        throw new InvalidOperationException("IN collection must be enumerable.");
                    return new ConditionExpression(crmCol, ConditionOperator.In, inValues.ToArray());
                }
            }
            // LIKE: property.Contains(value)
            if (mce.Object is MemberExpression memberObj && memberObj.Member is PropertyInfo propInfoObj)
            {
                if (propInfoObj.PropertyType == typeof(string))
                {
                    var crmCol = propInfoObj.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                    if (crmCol == null)
                        throw new InvalidOperationException($"Property {propInfoObj.Name} missing CrmColumnAttribute.");
                    var value = Expression.Lambda(mce.Arguments[0]).Compile().DynamicInvoke()?.ToString();
                    return new ConditionExpression(crmCol, ConditionOperator.Like, $"%{value}%");
                }
            }
        }
        else if (expr is MethodCallExpression mce2)
        {
            // StartsWith/EndsWith/Equals string functions
            if (mce2.Method.Name == "StartsWith" && mce2.Object is MemberExpression member && member.Member is PropertyInfo propInfoStr)
            {
                var crmCol = propInfoStr.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                if (crmCol == null)
                    throw new InvalidOperationException($"Property {propInfoStr.Name} missing CrmColumnAttribute.");
                var value = Expression.Lambda(mce2.Arguments[0]).Compile().DynamicInvoke()?.ToString();
                return new ConditionExpression(crmCol, ConditionOperator.Like, $"{value}%");
            }
            if (mce2.Method.Name == "EndsWith" && mce2.Object is MemberExpression member2 && member2.Member is PropertyInfo propInfoStr2)
            {
                var crmCol = propInfoStr2.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                if (crmCol == null)
                    throw new InvalidOperationException($"Property {propInfoStr2.Name} missing CrmColumnAttribute.");
                var value = Expression.Lambda(mce2.Arguments[0]).Compile().DynamicInvoke()?.ToString();
                return new ConditionExpression(crmCol, ConditionOperator.Like, $"%{value}");
            }
            if (mce2.Method.Name == "Equals" && mce2.Object is MemberExpression eqMember && eqMember.Member is PropertyInfo eqProp)
            {
                var crmCol = eqProp.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;
                if (crmCol == null)
                    throw new InvalidOperationException($"Property {eqProp.Name} missing CrmColumnAttribute.");
                var value = Expression.Lambda(mce2.Arguments[0]).Compile().DynamicInvoke();
                return new ConditionExpression(crmCol, ConditionOperator.Equal, value);
            }
        }
        else if (expr is UnaryExpression ue && ue.NodeType == ExpressionType.Not)
        {
            var inner = ExpressionToFilter(ue.Operand);
            if (inner is ConditionExpression ce)
            {
                ConditionOperator opp = ce.Operator switch
                {
                    ConditionOperator.Equal => ConditionOperator.NotEqual,
                    ConditionOperator.Null => ConditionOperator.NotNull,
                    ConditionOperator.Like => ConditionOperator.NotLike,
                    ConditionOperator.In => ConditionOperator.NotIn,
                    _ => throw new NotSupportedException($"Not on {ce.Operator} not supported.")
                };
                return new ConditionExpression(ce.AttributeName, opp, ce.Values?.ToArray());
            }
        }
        throw new NotSupportedException($"Expression type {expr.NodeType} is not supported for CRM-side filtering.");
    }

    private void BuildQueryPagination(QueryExpression query, Pagination? pagination)
    {
        if (pagination != null && pagination.PageNumber.HasValue && pagination.PageSize.HasValue)
        {
            query.PageInfo = new Microsoft.Xrm.Sdk.Query.PagingInfo
            {
                PageNumber = pagination.PageNumber.Value,
                Count = pagination.PageSize.Value,
                PagingCookie = null
            };
        }
    }

    private void BuildQueryFilter(QueryExpression query, Expression<Func<T, bool>> predicate)
    {
        if (predicate != null)
        {
            var filter = ExpressionToFilter(predicate.Body);
            if (filter is Microsoft.Xrm.Sdk.Query.ConditionExpression condition)
                query.Criteria.AddCondition(condition);
            else if (filter is Microsoft.Xrm.Sdk.Query.FilterExpression filterExpr)
                query.Criteria.AddFilter(filterExpr);
            else
                throw new NotSupportedException("Unknown filter type returned from parser.");
        }
    }



    // -------------------- Join-capable translator --------------------

    private QueryExpression BuildQueryWithJoins(Expression<Func<T, bool>> predicate, bool limitOne)
    {
        var columns = GetColumns();
        var query = new QueryExpression(_tableName)
        {
            ColumnSet = new ColumnSet(columns),
        };
        if (limitOne)
        {
            query.PageInfo = new PagingInfo { PageNumber = 1, Count = 1 };
        }

        // Simple rewrite: nav != null -> FK != null, nav.PK == c -> FK == c (fast path when applicable)
        var rewritten = RewriteNavPkAndNullToRootFk(predicate);

        var translator = new LinkJoinTranslator(
            rootQuery: query,
            rootParamType: typeof(T),
            rootTable: _tableName,
            getTableNameOf: GetEntityLogicalName,
            getPkOf: ResolvePrimaryKeyLogicalName
        );

        translator.Translate(rewritten.Body);
        return query;
    }

    private Expression<Func<T, bool>> RewriteNavPkAndNullToRootFk(Expression<Func<T, bool>> predicate)
    {
        var rewriter = new NavToFkRewriter(typeof(T), ResolveNavToFkMapForT(), ResolvePrimaryKeyLogicalName);
        var newBody = rewriter.Visit(predicate.Body);
        return Expression.Lambda<Func<T, bool>>(newBody, predicate.Parameters);
    }



    /// <summary>
    /// Recursively loads navigation properties (single and collections) for a list of entities.
    /// Handles both many-to-1 and 1-to-many relationships using reflection and custom attributes.
    /// Uses a processedTypes set to avoid infinite loops in cyclic graphs.
    /// Supports [CrmForeignKey] on base types for flexible navigation.
    /// </summary>
#pragma warning disable CS8600, CS8601, CS8602
    private async Task FillNavigationPropertiesBatchAsync(List<T> mainList, IServiceProvider sp, HashSet<Type> processedTypes)
    {
        if (mainList == null || mainList.Count == 0) return;
        processedTypes ??= new HashSet<Type>();
        if (!processedTypes.Add(typeof(T))) return; // prevent cycles per type

        var props = typeof(T).GetProperties();

        // --- N:1 (single navigation) ---
        var fkProps = props.Where(p => p.GetCustomAttribute<CrmForeignKeyAttribute>() != null).ToList();
        foreach (var fkProp in fkProps)
        {
            var fkAttr = fkProp.GetCustomAttribute<CrmForeignKeyAttribute>();
            if (fkAttr == null) continue;

            // Find matching navigation property by type compatibility (base/derived allowed)
            var navProp = props.FirstOrDefault(p => fkAttr.TargetEntityType.IsAssignableFrom(p.PropertyType));
            if (navProp == null) continue;

            // Collect distinct FK GUIDs
            var allIds = mainList
                .Select(x => fkProp.GetValue(x))
                .Select(value => value switch
                {
                    Guid g => g,
                    string s when Guid.TryParse(s, out var parsed) => parsed,
                    _ => (Guid?)null
                })
                .Where(g => g.HasValue)
                .Select(g => g!.Value)
                .Distinct()
                .ToList();

            if (allIds.Count == 0) continue;

            // Resolve navigation service for the nav type
            var navServiceType = typeof(IGenericCrmService<>).MakeGenericType(navProp.PropertyType);
            dynamic navService = sp.GetService(navServiceType);
            if (navService == null) continue;

            // Fetch related rows by IDs
            var relatedList = await navService.RetrieveByIdsAsync(allIds);

            // Find PK of navigation type to build a dictionary
            var targetKeyProp = navProp.PropertyType.GetProperties()
                .FirstOrDefault(p =>
                    fkAttr.TargetKey != null
                        ? p.Name == fkAttr.TargetKey
                        : (p.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName == ResolvePrimaryKeyLogicalName(navProp.PropertyType))
                );
            if (targetKeyProp == null) continue;

            // Build map: PK string -> related entity
            var relatedDict = new Dictionary<string, object>();
            foreach (var rel in relatedList)
            {
                var keyValue = targetKeyProp.GetValue(rel)?.ToString();
                if (!string.IsNullOrEmpty(keyValue) && !relatedDict.ContainsKey(keyValue))
                    relatedDict[keyValue] = rel;
            }

            // Assign first-level nav
            foreach (var main in mainList)
            {
                var fkValue = fkProp.GetValue(main)?.ToString();
                if (!string.IsNullOrEmpty(fkValue) && relatedDict.TryGetValue(fkValue, out var nav))
                    navProp.SetValue(main, nav);
            }

            // RECURSION for N:1: populate children of the related type too
            if (relatedList != null && relatedList.Count > 0)
            {
                var recursiveMethod = navService.GetType().GetMethod(
                    "FillNavigationPropertiesBatchAsync",
                    BindingFlags.NonPublic | BindingFlags.Instance,
                    binder: null,
                    types: new[] { typeof(List<>).MakeGenericType(navProp.PropertyType), typeof(IServiceProvider), typeof(HashSet<Type>) },
                    modifiers: null
                );
                if (recursiveMethod != null)
                {
                    await (Task)recursiveMethod.Invoke(navService, new object[] { relatedList, sp, processedTypes });
                }
            }
        }

        // --- 1:N (collection navigation) ---
        var collectionNavProps = props
            .Where(p =>
                p.PropertyType != typeof(string) &&
                p.PropertyType.IsGenericType &&
                p.PropertyType.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
            .ToList();

        foreach (var collectionNavProp in collectionNavProps)
        {
            var elementType = collectionNavProp.PropertyType.GetGenericArguments()[0];

            // Find child's FK to this T (allow base of T)
            var childFkProp = elementType.GetProperties().FirstOrDefault(p =>
            {
                var fka = p.GetCustomAttribute<CrmForeignKeyAttribute>();
                return fka != null && fka.TargetEntityType.IsAssignableFrom(typeof(T));
            });
            if (childFkProp == null) continue;

            var navServiceType = typeof(IGenericCrmService<>).MakeGenericType(elementType);
            dynamic navService = sp.GetService(navServiceType);
            if (navService == null) continue;

            var parentPkProp = typeof(T).GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName == ResolvePrimaryKeyLogicalName(typeof(T)));
            if (parentPkProp == null) continue;

            var parentKeys = mainList.Select(x => parentPkProp.GetValue(x)).Where(v => v != null).Distinct().ToList();

            // Fetch children filtered by FK IN parentKeys
            var childEntities = await navService.RetrieveByParentKeyAsync(childFkProp, parentKeys);

            // Group children by FK string
            var childDict = new Dictionary<string, List<object>>();
            List<object> list;
            foreach (var child in childEntities)
            {
                var fkVal = childFkProp.GetValue(child)?.ToString();
                if (string.IsNullOrEmpty(fkVal)) continue;
                if (!childDict.TryGetValue(fkVal, out list)) { list = new List<object>(); childDict[fkVal] = list; }
                list.Add(child);
            }

            // Assign collection to parent
            foreach (var parent in mainList)
            {
                var pk = parentPkProp.GetValue(parent)?.ToString();
                if (string.IsNullOrEmpty(pk)) continue;

                if (childDict.TryGetValue(pk, out var children))
                {
                    var casted = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(elementType)
                        .Invoke(null, new object[] { children });
                    var toList = typeof(Enumerable).GetMethod("ToList")!.MakeGenericMethod(elementType)
                        .Invoke(null, new object[] { casted });
                    collectionNavProp.SetValue(parent, toList);
                }
                else
                {
                    var emptyArray = Array.CreateInstance(elementType, 0);
                    var casted = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(elementType)
                        .Invoke(null, new object[] { emptyArray });
                    var toList = typeof(Enumerable).GetMethod("ToList")!.MakeGenericMethod(elementType)
                        .Invoke(null, new object[] { casted });
                    collectionNavProp.SetValue(parent, toList);
                }
            }

            // RECURSION for 1:N: populate deeper levels for the child type too
            if (childEntities != null && childEntities.Count > 0)
            {
                var recursiveMethod = navService.GetType().GetMethod(
                    "FillNavigationPropertiesBatchAsync",
                    BindingFlags.NonPublic | BindingFlags.Instance,
                    binder: null,
                    types: new[] { typeof(List<>).MakeGenericType(elementType), typeof(IServiceProvider), typeof(HashSet<Type>) },
                    modifiers: null
                );
                if (recursiveMethod != null)
                {
                    await (Task)recursiveMethod.Invoke(navService, new object[] { childEntities, sp, processedTypes });
                }
            }
        }
    }





    // -------------------- Basic CRUD helpers --------------------
#pragma warning restore CS8600, CS8601, CS8602


    /// <summary>
    /// Converts a CRM Entity to a POCO for use in .NET code.
    /// Handles EntityReference and type conversion for all mapped properties.
    /// </summary>
    private T ToPOCO(Entity entity)
    {
        var poco = new T();
        foreach (var prop in typeof(T).GetProperties())
        {
            var attr = prop.GetCustomAttribute<CrmColumnAttribute>();
            if (attr != null && entity.Contains(attr.LogicalName))
            {
                var value = entity[attr.LogicalName];
                if (value is EntityReference er)
                {
                    if (prop.PropertyType == typeof(Guid) || Nullable.GetUnderlyingType(prop.PropertyType) == typeof(Guid))
                        prop.SetValue(poco, er.Id);
                    else if (prop.PropertyType == typeof(string))
                        prop.SetValue(poco, er.Id.ToString());
                }
                else
                {
                    var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    prop.SetValue(poco, value == null ? null : Convert.ChangeType(value, targetType));
                }
            }
        }
        return poco;
    }

    private void ApplyPagination(QueryExpression query, Pagination? pagination)
    {
        if (pagination == null || !pagination.PageNumber.HasValue || !pagination.PageSize.HasValue) return;
        query.PageInfo = new PagingInfo
        {
            PageNumber = pagination.PageNumber.Value,
            Count = pagination.PageSize.Value
        };
    }

    /// <summary>
    /// Gets the logical names of all CRM columns for T using [CrmColumn] attributes.
    /// </summary>
    private string[] GetColumns() =>
        typeof(T).GetProperties()
            .Where(p => p.GetCustomAttribute<CrmColumnAttribute>() != null)
            .Select(p => p.GetCustomAttribute<CrmColumnAttribute>()!.LogicalName)
            .ToArray();

    private string GetEntityLogicalName(Type type)
    {
        var tableAttr = type.GetCustomAttribute<CrmTableAttribute>();
        if (tableAttr == null) throw new InvalidOperationException($"Missing CrmTable on {type.Name}");
        return tableAttr.TableName;
    }

    private string ResolvePrimaryKeyLogicalName(Type type)
    {
        var tableAttr = type.GetCustomAttribute<CrmTableAttribute>();
        if (tableAttr == null) throw new InvalidOperationException($"Missing CrmTable on {type.Name}");
        return tableAttr.TableName + "id";
    }

    private Dictionary<PropertyInfo, PropertyInfo> ResolveNavToFkMapForT()
    {
        var map = new Dictionary<PropertyInfo, PropertyInfo>();
        var props = typeof(T).GetProperties();

        var fkProps = props.Select(p => new { Prop = p, Attr = p.GetCustomAttribute<CrmForeignKeyAttribute>() })
                           .Where(x => x.Attr != null)
                           .ToList();

        foreach (var navProp in props)
        {
            var navType = navProp.PropertyType;
            if (navType == typeof(string)) continue;
            var fk = fkProps.FirstOrDefault(x => x.Attr!.TargetEntityType.IsAssignableFrom(navType));
            if (fk != null)
                map[navProp] = fk.Prop;
        }
        return map;
    }

    #endregion



    #region Private Classes
    // -------------------- Link join translator --------------------
    private sealed class LinkJoinTranslator
    {
        private readonly QueryExpression _rootQuery;
        private readonly string _rootTable;
        private readonly Func<Type, string> _tableOf;
        private readonly Func<Type, string> _pkOf;

        // Navigation path (e.g., "Application", "Application.Manager") -> LinkEntity
        private readonly Dictionary<string, LinkEntity> _links = new();

        // Current scope stack: each frame defines a param Type and a base pathKey
        private readonly Stack<(Type paramType, string? basePath)> _scope = new();

        public LinkJoinTranslator(QueryExpression rootQuery, Type rootParamType, string rootTable,
                                  Func<Type, string> getTableNameOf, Func<Type, string> getPkOf)
        {
            _rootQuery = rootQuery;
            _rootTable = rootTable;
            _tableOf = getTableNameOf;
            _pkOf = getPkOf;
            _scope.Push((rootParamType, null));
        }

        public void Translate(Expression expr)
        {
            Visit(expr);
        }

        private void Visit(Expression expr)
        {
            switch (expr)
            {
                case BinaryExpression be:
                    if (be.NodeType == ExpressionType.AndAlso)
                    {
                        Visit(be.Left);
                        Visit(be.Right);
                        return;
                    }
                    if (be.NodeType == ExpressionType.OrElse)
                    {
                        // Only safe when both sides target same scope
                        var leftScope = DetectScopeKey(be.Left);
                        var rightScope = DetectScopeKey(be.Right);
                        if (!Equals(leftScope, rightScope))
                            throw new NotSupportedException("OR across different navigation scopes is not supported.");

                        var filter = GetFilterForScope(leftScope);
                        var orFilter = new FilterExpression(LogicalOperator.Or);
                        AddExpressionToFilter(be.Left, orFilter);
                        AddExpressionToFilter(be.Right, orFilter);
                        if (orFilter.Conditions.Count > 0 || orFilter.Filters.Count > 0)
                            filter.AddFilter(orFilter);
                        return;
                    }

                    AddExpressionToFilter(be, GetFilterForScope(DetectScopeKey(be)));
                    return;

                case MethodCallExpression mce:
                    // Any on collections
                    if (IsAny(mce, out var sourceExpr, out var lambda))
                    {
                        if (!TryGetCollectionPath(sourceExpr, out var pathKey, out var itemType))
                            throw new NotSupportedException("Unsupported Any() source.");

                        // Ensure link chain for collection path
                        EnsureLinkChain(pathKey);

                        // Enter scope at the collection path with param of itemType
                        _scope.Push((itemType, pathKey));
                        try
                        {
                            // Translate child predicate into this path's LinkCriteria
                            var targetFilter = GetFilterForScope(new ScopeKey(pathKey));
                            AddExpressionToFilter(lambda.Body, targetFilter, lambda.Parameters[0]);
                        }
                        finally
                        {
                            _scope.Pop();
                        }
                        return;
                    }

                    AddExpressionToFilter(mce, GetFilterForScope(DetectScopeKey(mce)));
                    return;

                case UnaryExpression ue when ue.NodeType == ExpressionType.Not:
                    var notScope = DetectScopeKey(ue.Operand);
                    var notFilter = GetFilterForScope(notScope);
                    AddNegated(ue.Operand, notFilter);
                    return;

                default:
                    // Ignore others
                    return;
            }
        }

        // Adds a sub-expression into filter of current scope
        private void AddExpressionToFilter(Expression expr, FilterExpression filter, ParameterExpression? lambdaParam = null)
        {
            switch (expr)
            {
                case BinaryExpression be:
                    AddBinary(be, filter, lambdaParam);
                    return;
                case MethodCallExpression mce:
                    AddMethodCall(mce, filter, lambdaParam);
                    return;
                default:
                    return;
            }
        }

        // Inside LinkJoinTranslator
        // Adds a binary comparison to the appropriate FilterExpression.
        // Supports: ==, !=, >, >=, <, <=
        // Only column-to-value comparisons are supported (attribute-to-attribute not supported).
        private void AddBinary(BinaryExpression be, FilterExpression filter, ParameterExpression? lambdaParam)
        {
            // Determine which side is the column and which is the value
            if (!TryGetMemberSide(be.Left, be.Right, lambdaParam, out var memberSide, out var valueSide))
            {
                // Either both sides are columns or neither side is a column -> not supported here
                return;
            }

            // Resolve the CRM column and the navigation path (if any)
            if (!TryResolveColumn(memberSide, out var columnLogicalName, out var pathKey, lambdaParam))
                return;

            // Ensure the LinkEntity chain exists for this path (if not root)
            EnsureLinkChain(pathKey);

            // Handle null and not-null checks
            if (valueSide is ConstantExpression constExpr && constExpr.Value == null)
            {
                var nullOp = be.NodeType switch
                {
                    ExpressionType.Equal => ConditionOperator.Null,
                    ExpressionType.NotEqual => ConditionOperator.NotNull,
                    _ => (ConditionOperator?)null
                };
                if (nullOp != null)
                {
                    filter.AddCondition(new ConditionExpression(columnLogicalName, nullOp.Value));
                }
                return;
            }

            // Map the binary node type to a CRM operator
            var op = be.NodeType switch
            {
                ExpressionType.Equal => ConditionOperator.Equal,
                ExpressionType.NotEqual => ConditionOperator.NotEqual,
                ExpressionType.GreaterThan => ConditionOperator.GreaterThan,
                ExpressionType.GreaterThanOrEqual => ConditionOperator.GreaterEqual,
                ExpressionType.LessThan => ConditionOperator.LessThan,
                ExpressionType.LessThanOrEqual => ConditionOperator.LessEqual,
                _ => (ConditionOperator?)null
            };
            if (op == null) return;

            // Compile the RHS value (closure/constant/converted)
            var value = CompileValue(valueSide);

            // Emit the condition
            filter.AddCondition(new ConditionExpression(columnLogicalName, op.Value, value));
        }

        // Determines which side is a column (member path) and which side is a value.
        // Returns true if exactly one side resolves to a [CrmColumn] under the current scope (or lambdaParam scope).
        private bool TryGetMemberSide(Expression left, Expression right, out Expression memberSide, out Expression valueSide)
        {
            return TryGetMemberSide(left, right, /* lambdaParam: */ null, out memberSide, out valueSide);
        }

        // Determines which side is a column (member path) and which side is a value.
        // Returns true if exactly one side resolves to a [CrmColumn] under the current scope (or lambdaParam scope).
        private bool TryGetMemberSide(Expression left, Expression right, ParameterExpression? lambdaParam, out Expression memberSide, out Expression valueSide)
        {
            memberSide = null!;
            valueSide = null!;

            var leftIsCol = TryResolveColumn(left, out _, out _, lambdaParam);
            var rightIsCol = TryResolveColumn(right, out _, out _, lambdaParam);

            // Only support column-to-value comparisons (attribute-to-attribute not supported)
            if (leftIsCol && !rightIsCol)
            {
                memberSide = left;
                valueSide = right;
                return true;
            }
            if (rightIsCol && !leftIsCol)
            {
                memberSide = right;
                valueSide = left;
                return true;
            }

            return false;
        }

        private void AddMethodCall(MethodCallExpression mce, FilterExpression filter, ParameterExpression? lambdaParam)
        {
            // property.Contains(value) / StartsWith / EndsWith / Equals
            if (mce.Object != null && (mce.Method.Name == "Contains" || mce.Method.Name == "StartsWith" || mce.Method.Name == "EndsWith" || mce.Method.Name == "Equals"))
            {
                if (!TryResolveColumn(mce.Object, out var col, out var pathKey, lambdaParam))
                    return;

                EnsureLinkChain(pathKey);

                if (mce.Method.Name == "Equals")
                {
                    var v = CompileValue(mce.Arguments[0]);
                    filter.AddCondition(new ConditionExpression(col, ConditionOperator.Equal, v));
                    return;
                }

                var s = CompileValue(mce.Arguments[0])?.ToString();
                if (s == null) return;

                var like = mce.Method.Name switch
                {
                    "Contains" => $"%{s}%",
                    "StartsWith" => $"{s}%",
                    "EndsWith" => $"%{s}",
                    _ => null
                };
                if (like != null)
                    filter.AddCondition(new ConditionExpression(col, ConditionOperator.Like, like));
                return;
            }

            // collection.Contains(property) => IN
            if (mce.Method.Name == "Contains")
            {
                Expression? collectionExpr = null;
                Expression? itemExpr = null;
                if (mce.Object != null && mce.Arguments.Count == 1)
                {
                    collectionExpr = mce.Object;
                    itemExpr = mce.Arguments[0];
                }
                else if (mce.Object == null && mce.Arguments.Count == 2)
                {
                    collectionExpr = mce.Arguments[0];
                    itemExpr = mce.Arguments[1];
                }

                if (itemExpr != null && TryResolveColumn(itemExpr, out var col2, out var pathKey2, lambdaParam))
                {
                    EnsureLinkChain(pathKey2);
                    var collection = CompileValue(collectionExpr!);
                    var values = ToObjects(collection);
                    if (values.Length == 0) return;
                    filter.AddCondition(new ConditionExpression(col2, ConditionOperator.In, values));
                }
            }
        }

        private void AddNegated(Expression inner, FilterExpression filter)
        {
            // Basic negations mapping
            if (inner is BinaryExpression be)
            {
                if (!TryGetMemberSide(be.Left, be.Right, out var memberSide, out var valueSide)) return;
                if (!TryResolveColumn(memberSide, out var col, out var pathKey, /*lambdaParam*/ null)) return;
                EnsureLinkChain(pathKey);

                if (valueSide is ConstantExpression c && c.Value == null)
                {
                    var op = be.NodeType == ExpressionType.Equal ? ConditionOperator.NotNull : ConditionOperator.Null;
                    filter.AddCondition(new ConditionExpression(col, op));
                    return;
                }

                var op2 = be.NodeType switch
                {
                    ExpressionType.Equal => ConditionOperator.NotEqual,
                    ExpressionType.NotEqual => ConditionOperator.Equal,
                    ExpressionType.GreaterThan => ConditionOperator.LessEqual,
                    ExpressionType.GreaterThanOrEqual => ConditionOperator.LessThan,
                    ExpressionType.LessThan => ConditionOperator.GreaterEqual,
                    ExpressionType.LessThanOrEqual => ConditionOperator.GreaterThan,
                    _ => (ConditionOperator?)null
                };
                if (op2 == null) return;

                var val = CompileValue(valueSide);
                filter.AddCondition(new ConditionExpression(col, op2.Value, val));
                return;
            }

            if (inner is MethodCallExpression mce && mce.Method.Name == "Contains")
            {
                if (mce.Object != null && TryResolveColumn(mce.Object, out var col, out var pathKey, null))
                {
                    EnsureLinkChain(pathKey);
                    var s = CompileValue(mce.Arguments[0])?.ToString();
                    if (s == null) return;
                    filter.AddCondition(new ConditionExpression(col, ConditionOperator.NotLike, $"%{s}%"));
                    return;
                }

                // NOT collection.Contains(property) -> NotIn
                Expression? collectionExpr = null;
                Expression? itemExpr = null;
                if (mce.Object != null && mce.Arguments.Count == 1)
                {
                    collectionExpr = mce.Object;
                    itemExpr = mce.Arguments[0];
                }
                else if (mce.Object == null && mce.Arguments.Count == 2)
                {
                    collectionExpr = mce.Arguments[0];
                    itemExpr = mce.Arguments[1];
                }

                if (itemExpr != null && TryResolveColumn(itemExpr, out var col2, out var pathKey2, null))
                {
                    EnsureLinkChain(pathKey2);
                    var values = ToObjects(CompileValue(collectionExpr!));
                    if (values.Length == 0) return;
                    filter.AddCondition(new ConditionExpression(col2, ConditionOperator.NotIn, values));
                    return;
                }
            }
        }

        // ------------ Scope handling and resolution ------------

        private ScopeKey DetectScopeKey(Expression e)
        {
            if (TryResolveColumn(e, out _, out var path, null))
                return new ScopeKey(path);
            // Recurse for complex nodes to find any column usage
            switch (e)
            {
                case BinaryExpression be:
                    {
                        var l = DetectScopeKey(be.Left); if (l.PathKey != null) return l;
                        var r = DetectScopeKey(be.Right); if (r.PathKey != null) return r;
                        return new ScopeKey(null);
                    }
                case MethodCallExpression mce:
                    {
                        if (mce.Object != null)
                        {
                            var s = DetectScopeKey(mce.Object); if (s.PathKey != null) return s;
                        }
                        foreach (var a in mce.Arguments)
                        {
                            var s = DetectScopeKey(a); if (s.PathKey != null) return s;
                        }
                        return new ScopeKey(null);
                    }
                default:
                    return new ScopeKey(null);
            }
        }

        private FilterExpression GetFilterForScope(ScopeKey key)
        {
            // root scope
            if (key.PathKey == null) return _rootQuery.Criteria;

            // ensure link(s) and return deepest LinkCriteria
            return EnsureLinkChain(key.PathKey).LinkCriteria;
        }

        // Try to resolve expression to a [CrmColumn] and capture path key
        private bool TryResolveColumn(Expression expr, out string columnLogicalName, out string? pathKey, ParameterExpression? lambdaParam)
        {
            columnLogicalName = "";
            pathKey = null;

            if (!TryGetMemberPath(expr, out var ownerType, out var members, lambdaParam))
                return false;

            // members: [Nav1, Nav2, ..., Column]
            if (members.Count == 0) return false;
            var last = members[^1] as PropertyInfo;
            if (last == null) return false;

            var colAttr = last.GetCustomAttribute<CrmColumnAttribute>();
            if (colAttr == null) return false;

            columnLogicalName = colAttr.LogicalName;

            // Compute pathKey (if any navs)
            if (members.Count > 1)
            {
                var navs = members.Take(members.Count - 1).Cast<PropertyInfo>().Select(pi => pi.Name);
                var basePath = _scope.Peek().basePath;
                var segs = basePath == null ? navs : new[] { basePath }.Concat(navs);
                pathKey = string.Join(".", segs);
            }
            else
            {
                pathKey = _scope.Peek().basePath; // may be null (root) or set (inside Any over a collection)
            }

            return true;
        }

        // Returns true if expr is a path of MemberExpressions ending at either the current scope parameter or the root parameter
        private bool TryGetMemberPath(Expression expr, out Type ownerType, out List<MemberInfo> members, ParameterExpression? lambdaParam)
        {
            members = new List<MemberInfo>();
            ownerType = _scope.Peek().paramType;

            Expression? cur = expr;
            while (cur is MemberExpression me)
            {
                members.Add(me.Member);
                cur = me.Expression;
            }
            members.Reverse();

            if (cur is ParameterExpression pe)
            {
                // Accept either the current lambda param or the root param type in scope
                if (lambdaParam != null && pe == lambdaParam)
                {
                    ownerType = lambdaParam.Type;
                    return true;
                }
                if (pe.Type == _scope.Peek().paramType)
                {
                    ownerType = pe.Type;
                    return true;
                }
            }

            return false;
        }

        // Ensure the link chain for a pathKey exists and is attached to the root query
        private LinkEntity EnsureLinkChain(string? pathKey)
        {
            if (pathKey == null) return null!;

            if (_links.TryGetValue(pathKey, out var existing)) return existing;

            var segments = pathKey.Split('.');
            Type fromType = _scope.First().paramType; // root type
            string fromTable = _rootTable;

            LinkEntity? parent = null;
            string built = "";

            foreach (var seg in segments)
            {
                built = string.IsNullOrEmpty(built) ? seg : $"{built}.{seg}";
                if (_links.TryGetValue(built, out var found))
                {
                    parent = found;
                    fromType = GetPropertyType(fromType, seg);
                    fromTable = _tableOf(fromType);
                    continue;
                }

                var toType = GetPropertyType(fromType, seg);
                var toTable = _tableOf(toType);

                if (!TryGetRelationship(fromType, toType, out var fromAttr, out var toAttr))
                    throw new InvalidOperationException($"Cannot infer relationship from {fromType.Name} to {toType.Name} for nav '{seg}'.");

                var alias = built.Replace('.', '_');
                var link = new LinkEntity(fromTable, toTable, fromAttr, toAttr, JoinOperator.Inner)
                {
                    Columns = new ColumnSet(false),
                    EntityAlias = alias
                };

                // Attach to root or parent
                if (parent == null)
                {
                    _rootQuery.LinkEntities.Add(link);
                }
                else
                {
                    parent.LinkEntities.Add(link);
                }

                _links[built] = link;
                parent = link;
                fromType = toType;
                fromTable = toTable;
            }

            return _links[pathKey];
        }

        private static bool IsAny(MethodCallExpression mce, out Expression source, out LambdaExpression lambda)
        {
            source = null!;
            lambda = null!;
            if (mce.Method.Name != "Any") return false;

            if (mce.Arguments.Count == 2 && mce.Arguments[1] is LambdaExpression le)
            {
                source = mce.Arguments[0];
                lambda = le;
                return true;
            }

            if (mce.Object != null && mce.Arguments.Count == 1 && mce.Arguments[0] is LambdaExpression le2)
            {
                source = mce.Object;
                lambda = le2;
                return true;
            }

            return false;
        }

        private bool TryGetCollectionPath(Expression source, out string pathKey, out Type elementType)
        {
            pathKey = null!;
            elementType = null!;

            // source should be a member path to a collection nav
            if (!TryGetMemberPath(source, out var ownerType, out var members, null))
                return false;

            if (members.Count == 0) return false;
            var last = members[^1] as PropertyInfo;
            if (last == null) return false;

            var propType = last.PropertyType;
            if (!IsEnumerableOf(propType, out elementType)) return false;

            // path from current scope base + all members (they are navs up to the collection)
            var navs = members.Cast<PropertyInfo>().Select(pi => pi.Name);
            var basePath = _scope.Peek().basePath;
            pathKey = basePath == null ? string.Join(".", navs) : string.Join(".", new[] { basePath }.Concat(navs));

            return true;
        }

        private static bool IsEnumerableOf(Type t, out Type elementType)
        {
            elementType = null!;
            if (t == typeof(string)) return false;
            if (t.IsArray) { elementType = t.GetElementType()!; return true; }
            if (t.IsGenericType)
            {
                var ienum = t.GetInterfaces().Concat(new[] { t })
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
                if (ienum != null)
                {
                    elementType = ienum.GetGenericArguments()[0];
                    return true;
                }
            }
            return false;
        }

        private static object? CompileValue(Expression expr) => Expression.Lambda(expr).Compile().DynamicInvoke();

        private static object[] ToObjects(object? maybeEnumerable)
        {
            if (maybeEnumerable is IEnumerable e)
            {
                var list = new List<object>();
                foreach (var x in e) list.Add(x!);
                return list.ToArray();
            }
            return Array.Empty<object>();
        }

        private static Type UnwrapNavType(Type t)
        {
            if (t == typeof(string)) return t;

            // Array
            if (t.IsArray) return t.GetElementType()!;

            // IEnumerable<T> or concrete List<T>/ICollection<T>/etc.
            var ienum = t.GetInterfaces().Concat(new[] { t })
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (ienum != null)
                return ienum.GetGenericArguments()[0];

            return t;
        }

        // Returns the navigation entity type for the given property on ownerType.
        // If the property is a collection navigation, returns its element type.
        private Type GetPropertyType(Type ownerType, string propertyName)
        {
            var pi = ownerType.GetProperty(propertyName)
                     ?? throw new InvalidOperationException($"Property '{propertyName}' not found on {ownerType.Name}.");

            // Unwrap collections to element type so relationship detection works
            return UnwrapNavType(pi.PropertyType);
        }

        private bool TryGetRelationship(Type fromType, Type toType, out string fromAttribute, out string toAttribute)
        {
            // N:1 (fromType has FK to toType)
            var fkOnFrom = fromType.GetProperties()
                .Select(p => new { Prop = p, Fk = p.GetCustomAttribute<CrmForeignKeyAttribute>() })
                .FirstOrDefault(x => x.Fk != null && x.Fk.TargetEntityType.IsAssignableFrom(toType));
            if (fkOnFrom != null)
            {
                var fkCol = fkOnFrom.Prop.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName
                            ?? throw new InvalidOperationException($"FK property '{fkOnFrom.Prop.Name}' missing [CrmColumn] on {fromType.Name}");
                fromAttribute = fkCol;
                toAttribute = _pkOf(toType);
                return true;
            }

            // 1:N (toType has FK back to fromType)
            var fkOnTo = toType.GetProperties()
                .Select(p => new { Prop = p, Fk = p.GetCustomAttribute<CrmForeignKeyAttribute>() })
                .FirstOrDefault(x => x.Fk != null && x.Fk.TargetEntityType.IsAssignableFrom(fromType));
            if (fkOnTo != null)
            {
                var childFkCol = fkOnTo.Prop.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName
                                 ?? throw new InvalidOperationException($"FK property '{fkOnTo.Prop.Name}' missing [CrmColumn] on {toType.Name}");
                fromAttribute = _pkOf(fromType);
                toAttribute = childFkCol;
                return true;
            }

            fromAttribute = toAttribute = "";
            return false;
        }

        private sealed record ScopeKey(string? PathKey);
    }

    // -------------------- Nav->FK basic rewriter (fast path) --------------------
    private sealed class NavToFkRewriter : ExpressionVisitor

    {

        private readonly Type _rootType;

        private readonly Dictionary<PropertyInfo, PropertyInfo> _navToFkMap;

        private readonly Func<Type, string> _pkLogicalNameFor;

        public NavToFkRewriter(Type rootType,

                               Dictionary<PropertyInfo, PropertyInfo> navToFkMap,

                               Func<Type, string> pkLogicalNameFor)

        {

            _rootType = rootType;

            _navToFkMap = navToFkMap;

            _pkLogicalNameFor = pkLogicalNameFor;

        }

        protected override Expression VisitBinary(BinaryExpression node)

        {

            // 1) DO NOT rewrite nav != null / nav == null when FK is non-nullable (e.g., Guid).

            // Let the join translator enforce existence (via LinkEntity and child conditions).

            // If you really need it, only rewrite when FK is nullable or reference type.

            if ((node.NodeType == ExpressionType.NotEqual || node.NodeType == ExpressionType.Equal) &&

                (IsNavNullComparison(node.Left, node.Right) || IsNavNullComparison(node.Right, node.Left)))

            {

                var nav = (MemberExpression)((node.Left is MemberExpression) ? node.Left : node.Right);

                var fk = GetFkForNav((PropertyInfo)nav.Member);

                if (fk != null && IsNullableType(fk.PropertyType))

                {

                    // Safe to rewrite to FK != null (nullable/reference)

                    var param = (ParameterExpression)nav.Expression!;

                    var fkAccess = Expression.Property(param, fk);

                    var nullConst = Expression.Constant(null, fk.PropertyType);

                    return node.NodeType == ExpressionType.NotEqual

                        ? Expression.NotEqual(fkAccess, nullConst)

                        : Expression.Equal(fkAccess, nullConst);

                }

                // Skip rewrite for non-nullable FK; keep original so translator can handle join/child filter.

                return base.VisitBinary(node);

            }

            // 2) Rewrite nav.PK == value (or !=) -> FK == value (or !=), but only when leaf is the child's PK

            if (node.NodeType == ExpressionType.Equal || node.NodeType == ExpressionType.NotEqual)

            {

                if (TryGetNavPkAccess(node.Left, out var leftInfo) && TryConst(node.Right, out var rightVal))

                {

                    var rewritten = RewritePkComparison(leftInfo, rightVal, node.NodeType);

                    if (rewritten != null) return rewritten;

                }

                if (TryGetNavPkAccess(node.Right, out var rightInfo) && TryConst(node.Left, out var leftVal))

                {

                    var rewritten = RewritePkComparison(rightInfo, leftVal, node.NodeType);

                    if (rewritten != null) return rewritten;

                }

            }

            return base.VisitBinary(node);

        }

        private bool IsNavNullComparison(Expression a, Expression b)

        {

            // match: (a is nav on root) && (b is constant null)

            return a is MemberExpression me && IsRootNav(me) &&

                   b is ConstantExpression c && c.Value == null;

        }

        private bool IsRootNav(MemberExpression me)

        {

            return me.Expression is ParameterExpression pe && pe.Type == _rootType &&

                   me.Member is PropertyInfo pi && _navToFkMap.ContainsKey(pi);

        }

        private PropertyInfo? GetFkForNav(PropertyInfo navProp) =>

            _navToFkMap.TryGetValue(navProp, out var fk) ? fk : null;

        // IMPORTANT: only treat the leaf as PK if its CrmColumn logical name equals the child's PK logical name

        private bool TryGetNavPkAccess(Expression expr, out (PropertyInfo NavProp, PropertyInfo ChildPk, MemberExpression NavAccess) info)

        {

            info = default;

            if (expr is MemberExpression leaf && leaf.Member is PropertyInfo childProp
    && leaf.Expression is MemberExpression navAccess && navAccess.Member is PropertyInfo navProp
    && IsRootNav(navAccess))

            {

                var childType = navProp.PropertyType;

                var pkLogical = _pkLogicalNameFor(childType);

                var childCol = childProp.GetCustomAttribute<CrmColumnAttribute>()?.LogicalName;

                if (childCol == null) return false;

                if (!string.Equals(childCol, pkLogical, StringComparison.OrdinalIgnoreCase)) return false;

                info = (navProp, childProp, navAccess);

                return true;

            }

            return false;

        }

        // inside NavToFkRewriter
        private Expression? RewritePkComparison(
            (PropertyInfo NavProp, PropertyInfo ChildPk, MemberExpression NavAccess) pkInfo,
            object? value,
            ExpressionType nodeType)
        {
            var fk = GetFkForNav(pkInfo.NavProp);
            if (fk == null) return null;

            var param = (ParameterExpression)pkInfo.NavAccess.Expression!;
            // Build the left as base Expression (not MemberExpression variable) to allow ref Expression
            Expression left = Expression.Property(param, fk);

            // Build the right as a ConstantExpression, but hold it in an Expression variable
            var constCE = BuildConstantForType(value, left.Type);
            if (constCE == null) return null;
            Expression right = constCE;

            // Align types using base Expression refs
            AlignTypes(ref left, ref right);

            return nodeType == ExpressionType.Equal
                ? Expression.Equal(left, right)
                : Expression.NotEqual(left, right);
        }

        private static ConstantExpression? BuildConstantForType(object? value, Type targetType)

        {

            // Null constant

            if (value == null)

            {

                // null constant only valid for reference or nullable

                if (!IsNullableType(targetType) && targetType.IsValueType)

                    return null; // cannot build a null constant for non-nullable value types

                return Expression.Constant(null, targetType);

            }

            // Convert value to underlying type if target is Nullable<T>

            var nonNullableTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;

            try

            {

                object converted;

                if (nonNullableTarget == typeof(Guid))

                {

                    if (value is Guid g) converted = g;

                    else if (value is string s && Guid.TryParse(s, out var gs)) converted = gs;

                    else return null;

                }

                else

                {

                    converted = Convert.ChangeType(value, nonNullableTarget);

                }

                // If targetType is nullable, the constant must be of the nullable type to avoid arg-type mismatch

                if (nonNullableTarget != targetType)

                {

                    // Create a Constant of the non-nullable value and then rely on AlignTypes to insert a Convert

                    return Expression.Constant(converted, nonNullableTarget);

                }

                return Expression.Constant(converted, targetType);

            }

            catch

            {

                return null;

            }

        }

        private static void AlignTypes(ref Expression left, ref Expression right)

        {

            if (left.Type == right.Type) return;

            // If one is Nullable<T> and the other is T, convert the T to Nullable<T>

            if (IsNullableOf(left.Type, right.Type))

            {

                right = Expression.Convert(right, left.Type);

                return;

            }

            if (IsNullableOf(right.Type, left.Type))

            {

                left = Expression.Convert(left, right.Type);

                return;

            }

            // As a fallback, try to convert right to left's type

            if (CanConvert(right.Type, left.Type))

            {

                right = Expression.Convert(right, left.Type);

                return;

            }

            // Or convert left to right's type

            if (CanConvert(left.Type, right.Type))

            {

                left = Expression.Convert(left, right.Type);

                return;

            }

            // Else leave as-is; Expression.Equal will throw, but we avoided most cases above.

        }

        private static bool IsNullableType(Type t) =>

            !t.IsValueType || Nullable.GetUnderlyingType(t) != null;

        private static bool IsNullableOf(Type nullableType, Type valueType)

        {

            var underlying = Nullable.GetUnderlyingType(nullableType);

            return underlying != null && underlying == valueType;

        }

        private static bool CanConvert(Type from, Type to)

        {

            try

            {

                // Create a dummy convert expression to test

                Expression.Convert(Expression.Default(from), to);

                return true;

            }

            catch { return false; }

        }

        private static bool TryConst(Expression e, out object? value)

        {

            try { value = Expression.Lambda(e).Compile().DynamicInvoke(); return true; }

            catch { value = null; return false; }

        }

    }

    #endregion
}





/// <summary>
/// Exception type thrown when Dataverse operations fail within the generic CRM service.
/// </summary>
public sealed class GenericCrmServiceException : Exception
{
    public GenericCrmServiceException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Concrete implementation of IGenericCrmServiceFactory.
/// Resolves instances of IGenericCrmService&lt;T&gt; for any entity type T using dependency injection.
/// This pattern allows for clean, scalable service resolution without bloating constructors with many generic parameters.
/// </summary>
public class GenericCrmServiceFactory : IGenericCrmServiceFactory
{
    /// <summary>
    /// The IServiceProvider instance used to resolve generic services from the DI container.
    /// </summary>
    private readonly IServiceProvider _sp;

    /// <summary>
    /// Constructs a new GenericCrmServiceFactory using the given IServiceProvider.
    /// </summary>
    /// <param name="sp">The IServiceProvider used for resolving services.</param>
    public GenericCrmServiceFactory(IServiceProvider sp) => _sp = sp;

    /// <summary>
    /// Gets an IGenericCrmService&lt;T&gt; instance for the specified entity type T.
    /// </summary>
    /// <typeparam name="T">The entity type for which to resolve the CRM service.</typeparam>
    /// <returns>An IGenericCrmService&lt;T&gt; instance for the specified type.</returns>
    public IGenericCrmService<T> Get<T>() where T : class, new()
        => _sp.GetRequiredService<IGenericCrmService<T>>();
}




