using System;

namespace PerformanceBulkAndParallel
{
    public class Record
    {
        private static int _sequence = 1;

        public Record()
        {
            Id = _sequence;
            Description = Guid.NewGuid().ToString();

            _sequence++;
        }

        public int Id { get; set; }

        public string Description { get; set; }
    }
}
