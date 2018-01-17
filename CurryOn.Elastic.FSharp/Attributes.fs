namespace CurryOn.Elastic

type IndexedAttribute (typeName) =
    inherit Nest.ElasticsearchTypeAttribute(Name = typeName)

type AttachmentAttribute (name) =
    inherit Nest.AttachmentAttribute(Name = name)

type BinaryAttribute (name) =
    inherit Nest.BinaryAttribute(Name = name)

type BoolAttribute (name) =
    inherit Nest.BooleanAttribute(Name = name, Index = true)

type NonIndexedBoolAttribute (name) =
    inherit Nest.BooleanAttribute(Name = name, Index = false)

type AutoCompleteAttribute (name) =
    inherit Nest.CompletionAttribute(Name = name)

type DateAttribute (name, format) =
    inherit Nest.DateAttribute(Name = name, Index = true, Format = format)

type NonIndexedDateAttribute (name, format) =
    inherit Nest.DateAttribute(Name = name, Index = false, Format = format)

type IpAddressAttribute (name) =
    inherit Nest.IpAttribute(Name = name, Index = true)

type NonIndexedIpAddressAttribute (name) =
    inherit Nest.IpAttribute(Name = name, Index = false)

type ExactMatchAttribute (name) =
    inherit Nest.KeywordAttribute(Name = name, Index = true)

type NonIndexedExactMatchAttribute (name) =
    inherit Nest.KeywordAttribute(Name = name, Index = false)

type ObjectAttribute (name) =
    inherit Nest.ObjectAttribute(Name = name, Enabled = true)

type NonIndexedObjectAttribute (name) =
    inherit Nest.ObjectAttribute(Name = name, Enabled = false)

type ChildObjectAttribute (name, alsoFlatten) =
    inherit Nest.NestedAttribute(Name = name, Enabled = true, IncludeInParent = alsoFlatten)

type NonIndexedChildObjectAttribute (name, alsoFlatten) =
    inherit Nest.NestedAttribute(Name = name, Enabled = false, IncludeInParent = alsoFlatten)

type ByteAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Byte, Name = name, Index = true)

type NonIndexedByteAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Byte, Name = name, Index = false)

type IntegerAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Integer, Name = name, Index = true)

type NonIndexedIntegerAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Integer, Name = name, Index = false)

type Int16Attribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Short, Name = name, Index = true)

type NonIndexedInt16Attribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Short, Name = name, Index = false)

type Int64Attribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Long, Name = name, Index = true)

type NonIndexedInt64Attribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Long, Name = name, Index = false)

type FloatAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Double, Name = name, Index = true)

type NonIndexedFloatAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Double, Name = name, Index = false)

type SingleAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Float, Name = name, Index = true)

type NonIndexedSingleAttribute (name) =
    inherit Nest.NumberAttribute(Nest.NumberType.Float, Name = name, Index = false)

type FullTextAttribute (name) =
    inherit Nest.TextAttribute(Name = name, Index = true)

type NonIndexedFullTextAttribute (name) =
    inherit Nest.TextAttribute(Name = name, Index = false)