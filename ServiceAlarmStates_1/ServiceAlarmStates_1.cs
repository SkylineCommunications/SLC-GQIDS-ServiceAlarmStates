namespace ServiceAlarmStates_1
{
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net;
    using Skyline.DataMiner.Net.Messages;
    using System.Collections.Generic;
    using System.Linq;

    [GQIMetaData(Name = "Service alarm states")]
    public sealed class ServiceAlarmStates : IGQIDataSource, IGQIOnInit, IGQIInputArguments, IGQIUpdateable
    {
        private readonly GQIStringColumn _nameColumn = new GQIStringColumn("Name");
        private readonly GQIStringColumn _stateColumn = new GQIStringColumn("Alarm state");

        private readonly GQIIntArgument _viewIdArgument = new GQIIntArgument("View ID");

        private GQIDMS _dms = null;
        private int _viewId = -1;
        private IGQIUpdater _updater;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            _dms = args.DMS;
            return default;
        }

        public GQIArgument[] GetInputArguments()
        {
            return new[] { _viewIdArgument };
        }

        public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
        {
            if (args.TryGetArgumentValue(_viewIdArgument, out int viewId))
                _viewId = viewId;
            return default;
        }

        public GQIColumn[] GetColumns()
        {
            return new[]
            {
                _nameColumn,
                _stateColumn,
            };
        }

        public void OnStartUpdates(IGQIUpdater updater)
        {
            _updater = updater;
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var services = GetServices(_viewId);
            var rows = new GQIRow[services.Length];
            for (int i = 0; i < services.Length; i++)
            {
                var service = (LiteServiceInfoEvent)services[i];
                var rowKey = ElementID.GetKey(service.DataMinerID, service.ID);
                var cells = new[]
                {
                    new GQICell { Value = service.Name },
                    new GQICell { Value = null },
                };

                rows[i] = new GQIRow(rowKey, cells);
            }


            if (_updater != null)
                SubscribeOnServiceAlarmStates(_updater, rows);
            else
                SetServiceAlarmStates(rows);

            return new GQIPage(rows)
            {
                HasNextPage = false,
            };
        }

        public void OnStopUpdates() { }

        private DMSMessage[] GetServices(int viewId)
        {
            var request = new GetLiteServiceInfo { ViewID = viewId };
            return _dms.SendMessages(request);
        }

        private void SubscribeOnServiceAlarmStates(IGQIUpdater updater, GQIRow[] rows)
        {
            var rowKeys = new HashSet<string>(rows.Select(row => row.Key));
            var connection = _dms.GetConnection();

            connection.OnNewMessage += (object sender, NewMessageEventArgs e) =>
            {
                if (!(e.Message is LiteServiceStateEvent stateEvent))
                    return;

                var rowKey = ElementID.GetKey(stateEvent.DataMinerID, stateEvent.ServiceID);
                if (!rowKeys.Contains(rowKey))
                    return;

                var updatedCell = new GQICell { Value = stateEvent.Level.ToString() };
                updater.UpdateCell(rowKey, _stateColumn, updatedCell);
            };

            var subscriptionFilter = new SubscriptionFilter(typeof(LiteServiceStateEvent));
            connection.Subscribe(subscriptionFilter);
        }

        private void SetServiceAlarmStates(GQIRow[] rows)
        {
            var rowLookUp = rows.ToDictionary(row => row.Key);
            var states = GetServiceStates();

            foreach (var state in states)
            {
                var key = ElementID.GetKey(state.DataMinerID, state.ElementID);
                if (!rowLookUp.TryGetValue(key, out var row))
                    continue;
                row.Cells[1].Value = state.Level.ToString();
            }
        }

        private IEnumerable<GetAlarmStateResponseMessage> GetServiceStates()
        {
            var request = new GetAlarmStateAllMessage
            {
                Elements = false,
                Services = true,
            };
            return _dms.SendMessages(request).Cast<GetAlarmStateResponseMessage>();
        }
    }
}
