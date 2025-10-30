# DynamicsXchangeToolkit

`DynamicsXchangeToolkit` is a reusable .NET library that packages the `GenericCrmService<T>` primitive together with helper attributes and utilities for Microsoft Dataverse (Dynamics 365/CRM). It targets any POCO entity decorated with `[CrmTable]`, `[CrmColumn]`, and `[CrmForeignKey]` attributes and surfaces rich CRUD plus navigation-loading support on top of the Power Platform Dataverse `ServiceClient`.

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
[CrmTable("adio_esp_applications")]
public class Application { ... }
```


### [CrmColumn]
Maps a property to a CRM column (logical name).
```csharp
[CrmColumn("adio_esp_applicationsid")]
public Guid CRM_AppId { get; set; }
```


### [CrmForeignKey]
Indicates a property is a foreign key to another entity type.
```csharp
[CrmForeignKey(typeof(ApplicationState))]
public Guid StateId { get; set; }
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
	>
	> **Example:**
	> - If you load an Application, and Application has a collection of CbQuotations, and each CbQuotation has a reference back to Application, the loader will:
	>   - Load the Application.
	>   - Load all CbQuotations for that Application.
	>   - For each CbQuotation, see the Application reference, but since Application is already in the processed set, it will not reload or recurse further, preventing an infinite loop.

---
## Predicate Translation & Joins

- **Server-Side Filtering**: Methods like `RetrieveAllAsync(predicate)` and `RetrieveWithNavigationBatchByPredicateAsync` turn your LINQ expression into CRM `ConditionExpression` and `LinkEntity` objects; data is filtered inside Dataverse, not in memory.
- **Navigation Joins**: Expressions that touch navigation properties (e.g. `app.State.StatusCode == 1`) automatically create the required joins and translate into the target entity's logical columns.
- **Collection Support**: `.Any()` on collection navigations is supported and scoped to the related `LinkEntity`, letting you write filters such as `app.CbQuotations.Any(q => q.Amount > threshold)`.
- **Pattern Coverage**: Equality/inequality, comparison operators, null checks, `Contains`, `StartsWith`, `EndsWith`, and `Not` are converted to their Dataverse equivalents.
- **Current Limitations**: `OR` spanning different navigation paths and attribute-to-attribute comparisons are not supported; keep complex disjunctions within the same navigation scope.

### Under the hood

- **LinkJoinTranslator** walks the expression tree after any lightweight rewrites and emits the Dataverse `QueryExpression` structure. It discovers which navigation paths the filter touches, materializes the required `LinkEntity` chain for each path, and attaches the translated conditions to the right `FilterExpression` scope so the query executes server-side.
- **NavToFkRewriter** runs first and simplifies common root-level navigation comparisons. When it sees a safe pattern such as `app.Investor.CrmId == investorId`, it rewrites it to the foreign key (`app.InvestorId == investorId`), reducing the amount of joins the translator has to add. It skips non-nullable FK null checks so join-based existence filters continue to behave correctly.

```csharp
// Example: fetch first application with a submitted quotation from a specific investor
var app = await applicationService.RetrieveWithNavigationBatchByPredicateAsync(
    sp,
    a => a.Investor.CrmId == investorId
         && a.CbQuotations.Any(q => q.StatusCode == (int)QuotationStatus.Submitted));
```

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

