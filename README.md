# DynamicsXchangeToolkit

`DynamicsXchangeToolkit` is a reusable .NET library that packages the `GenericCrmService<T>` primitive together with helper attributes and utilities for Microsoft Dataverse (Dynamics 365/CRM). It targets any POCO entity decorated with `[CrmTable]`, `[CrmColumn]`, and `[CrmForeignKey]` attributes and surfaces rich CRUD plus navigation-loading support on top of the Power Platform Dataverse `ServiceClient`. The library builds against `.NET 8.0`.

---

## How It Works

- **Reflection-Based Mapping**: The service uses .NET reflection to discover table and column mappings at runtime, based on custom attributes you place on your POCO classes.
- **Attribute-Driven**: You must decorate your entity classes and properties with `[CrmTable]`, `[CrmColumn]`, and `[CrmForeignKey]` attributes to enable correct mapping and navigation loading.
- **Navigation Loading**: The service can recursively load related entities (single and collections) by analyzing navigation properties and foreign key attributes, avoiding cycles and supporting inheritance-safe `[CrmForeignKey]` usage.
- **Expression-Driven Queries**: Predicate overloads translate `Expression<Func<T,bool>>` filters into Dataverse `QueryExpression` objects, so filtering happens server-side (including joins across navigation paths).
- **Service Provider**: Navigation loading uses an `IServiceProvider` to resolve other `IGenericCrmService<>` instances for related types.

---

## Custom Attribute Reference


### [CrmTable]
Marks a class as a CRM table and provides the logical table name.
```csharp
[CrmTable("tablea")]
public class Table_A { ... }
```


### [CrmColumn]
Maps a property to a CRM column (logical name).
```csharp
[CrmColumn("tableaid")]
public Guid CRM_Table_A_Id { get; set; }
```


### [CrmForeignKey]
Indicates a property is a foreign key to another entity type.
```csharp
[CrmForeignKey(typeof(Table_B))]
public Guid Table_B_Id { get; set; }
```

---

## Navigation Loading Explained

- **Single Navigation (Many-to-1)**: If a property is marked with `[CrmForeignKey]`, the service will load the related entity and assign it to the navigation property.
- **Collection Navigation (1-to-Many)**: If a property is a collection of another entity, the service will find the foreign key on the child type and load all related children.
- **Recursive Loading**: The service will recursively load navigation properties for related entities, but avoids infinite loops using a processed types set.

	**Deep Dive:**
	> When you call a navigation-loading method (such as `RetrieveWithNavigationBatchByIdAsync`), the service inspects all navigation properties (single and collections) on your entity. For each navigation property:
	>
	> 1. **Single Navigation (Many-to-1):**
	>    - The service finds all unique foreign key values in your main list.
	>    - It uses the DI service provider to resolve the correct `IGenericCrmService<>` for the related type.
	>    - It loads all related entities in a single batch call.
	>    - It assigns each related entity to the correct navigation property on your main objects.
	>    - **Recursion:** For each related entity, the service again inspects its navigation properties and loads their related entities, and so on.
	>
	> 2. **Collection Navigation (1-to-Many):**
	>    - The service finds all parent keys in your main list.
	>    - It loads all child entities whose foreign key matches any parent key.
	>    - It groups and assigns the child entities to the correct collection property on each parent.
	>    - **Recursion:** For each child entity, the service again inspects and loads its navigation properties.
	>
	> 3. **Cycle Prevention:**
	>    - To avoid infinite loops (e.g., A → B → A → ...), the service keeps a `HashSet<Type>` of all types it has already processed in the current recursion chain.
	>    - If it encounters a type already in the set, it skips further recursion for that branch.
	>    - This ensures that even with circular references (e.g., Account → Contact → Account), the process will terminate safely.
	>
	> 4. **Efficiency:**
	>    - All navigation loading is done in batches (not one-by-one), minimizing CRM round-trips.
	>    - The recursion is depth-first, so all levels of navigation are loaded as needed, but never repeated for the same type in the same chain.


---
## Predicate Translation & Joins

- **Server-Side Filtering**: Methods like `RetrieveAllAsync(predicate)` and `RetrieveWithNavigationBatchByPredicateAsync` turn your LINQ expression into CRM `ConditionExpression` and `LinkEntity` objects; data is filtered inside Dataverse, not in memory.
- **Navigation Joins**: Expressions that touch navigation properties (e.g. `tableA.StateColumn.StatusCode == 1`) automatically create the required joins and translate into the target entity's logical columns.
- **Collection Support**: `.Any()` on collection navigations is supported and scoped to the related `LinkEntity`, letting you write filters such as `tableA.RelatedTableBs.Any(b => b.Amount > threshold)`.
- **Pattern Coverage**: Equality/inequality, comparison operators, null checks, `Contains`, `StartsWith`, `EndsWith`, and `Not` are converted to their Dataverse equivalents.
- **Current Limitations**: `OR` spanning different navigation paths and attribute-to-attribute comparisons are not supported; keep complex disjunctions within the same navigation scope.

### Under the hood

- **LinkJoinTranslator** walks the expression tree after any lightweight rewrites and emits the Dataverse `QueryExpression` structure. It discovers which navigation paths the filter touches, materializes the required `LinkEntity` chain for each path, and attaches the translated conditions to the right `FilterExpression` scope so the query executes server-side.
- **NavToFkRewriter** runs first and simplifies common root-level navigation comparisons. When it sees a safe pattern such as `tableA.RelatedTableB.TableBId == relatedTableBId`, it rewrites it to the foreign key (`tableA.RelatedTableBId == relatedTableBId`), reducing the amount of joins the translator has to add. It skips non-nullable FK null checks so join-based existence filters continue to behave correctly.

```csharp
// Example: fetch the first TableA row with a submitted related TableC record from a specific TableB
var tableARecord = await tableAService.RetrieveWithNavigationBatchByPredicateAsync(
	sp,
	a => a.RelatedTableB.TableBId == relatedTableBId
		 && a.RelatedTableCs.Any(c => c.StatusCode == (int)TableCStatus.Submitted));

// Example: resolve services dynamically with the factory
var factory = serviceProvider.GetRequiredService<IGenericCrmServiceFactory>();
var tableBService = factory.Get<TableB>();
var tableB = await tableBService.RetrieveAsync(tableBId);
```

`GenericCrmServiceFactory` acts as a thin wrapper over the DI container so you can resolve `IGenericCrmService<T>` when `T` is only known at runtime (for example, inside navigation-loading routines or generic business logic). If you know the concrete entity type at compile time, prefer constructor-injecting `IGenericCrmService<T>` directly for that class. Use the factory when you need late-bound access to the toolkit for multiple entity types without enumerating every generic variation ahead of time.

---

## Error Handling & Best Practices

- All public methods wrap Dataverse failures in `GenericCrmServiceException` so callers can handle errors consistently.
- Check for `null` returns on retrieval methods that may legitimately not find data (e.g., predicate overloads).
- Use try/catch around service calls to surface meaningful messages or retry behavior in your application code.
- Ensure every entity type is decorated with the required attributes; missing metadata throws immediately to prevent silent data issues.

---

## Advanced Usage

- **Custom Filtering**: Use the predicate-enabled overloads to generate server-side `QueryExpression` filters, including navigation joins via property access or `.Any()`.
- **Custom Paging**: Use the `Pagination` parameter to retrieve data in pages.
- **Extending**: You can inherit from `GenericCrmService<T>` to add custom business logic or override mapping behavior.
- **Troubleshooting**: If navigation loading fails, check that all related types are registered in your DI container and decorated with attributes.

---

## Dependency Injection Setup

Register the toolkit services in your application startup so they can be resolved from DI for any POCO entity:

```csharp
// Register ServiceClient (singleton)
builder.Services.AddSingleton<ServiceClient>(sp =>
{
	var configuration = sp.GetRequiredService<IConfiguration>();
	var connectionString = configuration.GetSection("Dynamics365:ConnectionString").Value;
	return new ServiceClient(connectionString);
});

// Register the open generic for all POCOs using the interface
builder.Services.AddScoped(typeof(IGenericCrmService<>), typeof(GenericCrmService<>));
builder.Services.AddScoped<IGenericCrmServiceFactory, GenericCrmServiceFactory>();
```

`GenericCrmServiceFactory` (implemented in `GenericCrmService.cs`) wraps the DI container and exposes a single `Get<T>()` method. This allows you to resolve `IGenericCrmService<T>` for any entity type at runtime without knowing the generic type argument at compile time.

---

## Required Packages

Install the following NuGet packages in your project before referencing the toolkit:

- `Microsoft.PowerPlatform.Dataverse.Client`
- `Microsoft.Extensions.DependencyInjection.Abstractions`

These dependencies provide the Dataverse `ServiceClient` and the DI primitives used by the toolkit. Add them via `dotnet add package` or your IDE's NuGet package manager.

---

## FAQ

**Q: Can I use this with any CRM entity?**
A: Yes, as long as you decorate your POCO with the correct attributes and provide the logical names.

**Q: Does it support deep navigation (multi-level)?**
A: Yes, navigation loading is recursive and will load related entities to any depth, avoiding cycles.

**Q: What if my entity has a composite key?**
A: The service assumes a single primary key per entity, as is standard in Dataverse.

**Q: How do I register this in DI?**
A: Register `IGenericCrmService<T>` for each entity type in your DI container, passing the correct `ServiceClient`.

---

