using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Microsoft.Azure.EventHubs.Tests.ServiceFabricProcessor
{
    public class SFPtestbase
    {
        public SFPtestbase()
        {
        }

        [Fact]
        [DisplayTestMethodName]
        void SmokeTest()
        {
            TestUtility.Log("Hello world");
        }
    }
}
