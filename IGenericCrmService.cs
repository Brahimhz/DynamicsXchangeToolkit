using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace DynamicsXchangeToolkit;

public interface IGenericCrmService<T> where T : class, new()
{
    Task<Guid?> CreateAsync(T entity);
    Task<T> RetrieveAsync(Guid id);
    Task<T?> RetrieveAsync(Expression<Func<T, bool>> predicate);
    Task<bool> UpdateAsync(Guid id, T entity);
    Task DeleteAsync(Guid id);
    Task<List<T>> RetrieveAllAsync(Pagination? pagination = null);
    Task<List<T>> RetrieveAllAsync(Expression<Func<T, bool>> predicate, Pagination? pagination = null);
    Task<List<T>> RetrieveByIdsAsync(IEnumerable<Guid> ids);


    Task<T?> RetrieveWithNavigationBatchByIdAsync(Guid id, IServiceProvider sp);
    Task<List<T>> RetrieveAllWithNavigationBatchAsync(IServiceProvider sp, Pagination? pagination = null);
    Task<List<T>> RetrieveAllWithNavigationBatchAsync(IServiceProvider sp, Expression<Func<T, bool>> predicate, Pagination? pagination = null);
    Task<T?> RetrieveWithNavigationBatchByPredicateAsync(IServiceProvider sp, Expression<Func<T, bool>> predicate);

    // For 1-to-many support
    Task<List<T>> RetrieveByParentKeyAsync(PropertyInfo childFkProp, IEnumerable<object?> parentKeys);
}




/// <summary>
/// Factory interface for resolving generic CRM services for any entity type.
/// Provides a type-safe way to get an IGenericCrmService&lt;T&gt; implementation at runtime,
/// allowing dynamic and scalable access to CRUD and navigation operations for various entities.
/// </summary>
public interface IGenericCrmServiceFactory
{
    /// <summary>
    /// Gets an instance of IGenericCrmService&lt;T&gt; for the specified entity type T.
    /// </summary>
    /// <typeparam name="T">Entity type for which the CRM service is requested. Must be a class with a parameterless constructor.</typeparam>
    /// <returns>An IGenericCrmService&lt;T&gt; instance for the specified entity type.</returns>
    IGenericCrmService<T> Get<T>() where T : class, new();
}
