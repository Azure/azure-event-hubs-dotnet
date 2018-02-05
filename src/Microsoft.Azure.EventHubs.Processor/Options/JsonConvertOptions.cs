using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Microsoft.Azure.EventHubs.Processor.Options
{
    /// <summary>
    /// JsonConvert Options
    /// </summary>
    public static class JsonConvertOptions
    {
        /// <summary>
        /// Get the JsonConvert formatting options. so it's not conflicting / dependent upon the user's code.
        /// </summary>
        /// <returns></returns>
        public static JsonSerializerSettings GetOptions()
        {
            return new JsonSerializerSettings
            {
                Formatting = Formatting.Indented,
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                NullValueHandling = NullValueHandling.Ignore
            };
        }
    }
}