using DynamicsXchangeToolkit.Attributes;
using DynamicsXchangeToolkit.Helpers;
using Xunit;

namespace DynamicsXchangeToolkit.Tests;

public class CrmUtilsTests
{
    [Fact]
    public void GetTableLogicalName_ReturnsMappedName()
    {
        var logicalName = CrmUtils.GetTableLogicalName<SampleEntity>();

        Assert.Equal("sample_table", logicalName);
    }

    [Fact]
    public void GetTableLogicalName_ReturnsEmptyWhenMissingAttribute()
    {
        var logicalName = CrmUtils.GetTableLogicalName<UnmappedEntity>();

        Assert.Equal(string.Empty, logicalName);
    }

    [CrmTable("sample_table")]
    private class SampleEntity
    {
    }

    private class UnmappedEntity
    {
    }
}
