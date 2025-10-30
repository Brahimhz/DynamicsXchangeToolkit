using System;
using System.Reflection;
using Microsoft.Xrm.Sdk;
using DynamicsXchangeToolkit.Attributes;

namespace DynamicsXchangeToolkit.Helpers;

public static class CrmUtils
{
    public static string GetTableLogicalName<T>() where T : class
    {
        try
        {
            return typeof(T).GetCustomAttribute<CrmTableAttribute>()?.TableName
                ?? throw new InvalidOperationException("Missing CrmTable attribute.");
        }
        catch (Exception)
        {
            return string.Empty;
        }
        
    }

    public static EntityReference CreateLookup<T>(Guid id) where T : class
    {
        return new EntityReference(GetTableLogicalName<T>(), id);
    }
}
