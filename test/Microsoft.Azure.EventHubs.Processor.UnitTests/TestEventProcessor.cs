namespace Microsoft.Azure.EventHubs.Processor.UnitTests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class TestEventProcessor : IEventProcessor
    {
        public event EventHandler<PartitionContext> OnOpen;
        public event EventHandler<Tuple<PartitionContext, CloseReason>> OnClose;
        public event EventHandler<Tuple<PartitionContext, ReceivedEventArgs>> OnProcessEvents;
        public event EventHandler<Tuple<PartitionContext, Exception>> OnProcessError;

        public TestEventProcessor()
        {
        }

        Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.OnClose?.Invoke(this, new Tuple<PartitionContext, CloseReason>(context, reason));
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception error)
        {
            this.OnProcessError?.Invoke(this, new Tuple<PartitionContext, Exception>(context, error));
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> events)
        {
            var eventsArgs = new ReceivedEventArgs();
            eventsArgs.events = events;
            this.OnProcessEvents?.Invoke(this, new Tuple<PartitionContext, ReceivedEventArgs>(context, eventsArgs));
            EventData lastEvent = events?.LastOrDefault();

            // Checkpoint with last event?
            if (eventsArgs.checkPointLastEvent && lastEvent != null)
            {
                return context.CheckpointAsync(lastEvent);
            }

            // Checkpoint batch? This should checkpoint with last message delivered.
            if (eventsArgs.checkPointBatch)
            {
                return context.CheckpointAsync();
            }

            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.OnOpen?.Invoke(this, context);
            return Task.FromResult<object>(null);
        }
    }

    class TestEventProcessorFactory : IEventProcessorFactory
    {
        public event EventHandler<Tuple<PartitionContext, TestEventProcessor>> OnCreateProcessor;

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext context)
        {
            var processor = new TestEventProcessor();
            this.OnCreateProcessor?.Invoke(this, new Tuple<PartitionContext, TestEventProcessor>(context, processor));
            return processor;
        }
    }

    class ReceivedEventArgs
    {
        public IEnumerable<EventData> events;
        public bool checkPointLastEvent = true;
        public bool checkPointBatch = false;
    }
}
