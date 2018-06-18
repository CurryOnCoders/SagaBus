namespace CurryOn.Elastic

open FSharp.Control
open FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System.Reflection

type ElasticsearchProvider (config: TypeProviderConfig) as this =
    inherit TypeProviderForNamespaces()

    let assembly = Assembly.GetExecutingAssembly()
    let rootNamespace = "CurryOn.Elastic.Provider"
    let parameter = [ProvidedStaticParameter("settings", typeof<ElasticSettings>)]
    let baseType = Some typeof<obj>
    let indicesType = ProvidedTypeDefinition(assembly, rootNamespace, "Indices", baseType)

    do indicesType.DefineStaticParameters(parameters = parameter, instantiationFunction =
        (fun typeName parameterValues ->
            match parameterValues with
            | [| :? ElasticSettings as settings |] ->
                operation {
                    let client = Elasticsearch.getClient settings
                    let indices = ProvidedTypeDefinition(assembly, rootNamespace, typeName, baseType)
                    let! indexMappings = Elastic.getMappings client
                    indexMappings
                    |> Map.map (fun key index ->
                        let indexType = ProvidedTypeDefinition(assembly, rootNamespace, key, baseType)
                        index
                        |> Map.iter (fun typeName typeMapping ->
                            let recordType = ProvidedTypeDefinition(assembly, rootNamespace, typeName, baseType)
                            typeMapping.Properties
                            |> Seq.map (fun property -> property.Value)
                            |> Seq.map (fun property -> ProvidedProperty(property.Name.Name, property.Type.Type))
                            //|> Seq.map (fun property -> property.)
                            |> Seq.iter (fun property -> recordType.AddMember(property))
                        )
                        indexType
                    ) |> Map.iter (fun key value -> indices.AddMember(value))
                    return indices
                } |> Operation.returnOrFail
            | _ -> failwith "ElasticsearchProvider requires a single ElasticSettings parameter"
        ))

    do this.AddNamespace(rootNamespace, [indicesType])
        
    
//[<TypeProviderAssembly>]
//do ()