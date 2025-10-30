using System;

namespace DynamicsXchangeToolkit.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class CrmTableAttribute : Attribute
{
    public string TableName { get; }
    public CrmTableAttribute(string tableName) => TableName = tableName;
}

[AttributeUsage(AttributeTargets.Property)]
public class CrmColumnAttribute : Attribute
{
    public string LogicalName { get; }
    public CrmColumnAttribute(string logicalName) => LogicalName = logicalName;
}

[AttributeUsage(AttributeTargets.Property)]
public class CrmForeignKeyAttribute : Attribute
{
    public Type TargetEntityType { get; }
    public string? TargetKey { get; }
    public CrmForeignKeyAttribute(Type targetEntityType, string? targetKey = null)
    {
        TargetEntityType = targetEntityType;
        TargetKey = targetKey;
    }
}