// SAGE SSE Event Stream Client

export class SSEClient {
    constructor(url = '/v1/dashboard/events') {
        this.url = url;
        this.listeners = {};
        this.es = null;
        this.connected = false;
        this.reconnectDelay = 1000;
        this._reconnectTimer = null;
        this._closed = false;
    }

    connect() {
        if (this._closed) return;
        this.es = new EventSource(this.url);

        this.es.onopen = () => {
            this.connected = true;
            this.reconnectDelay = 1000;
            this._emit('connection', { connected: true });
        };

        this.es.onerror = () => {
            this.connected = false;
            this._emit('connection', { connected: false });
            this.es.close();
            this._reconnectTimer = setTimeout(() => this.connect(), this.reconnectDelay);
            this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
        };

        // Every event the node emits must be listed here: EventSource only
        // delivers named events to listeners that were explicitly registered, so
        // an omitted name is emitted by the server and seen by nobody. This list
        // is kept in lockstep with web.AllEventTypes (web/sse.go) by
        // TestSSEEventWiring — add an event there and here, or the build fails.
        const eventTypes = [
            'remember', 'recall', 'forget', 'vote', 'consensus', 'agent',
            'access', 'update', 'governance', 'import', 'task', 'recovery',
            'reinstate', 'cocommit', 'search', 'hybrid',
            'pipeline_send', 'pipeline_complete', 'redeploy',
        ];
        for (const type of eventTypes) {
            this.es.addEventListener(type, (e) => {
                try {
                    const data = JSON.parse(e.data);
                    this._emit(type, data);
                    this._emit('any', data);
                } catch (err) {
                    // ignore parse errors
                }
            });
        }
    }

    on(event, callback) {
        if (!this.listeners[event]) this.listeners[event] = [];
        this.listeners[event].push(callback);
        return () => {
            this.listeners[event] = this.listeners[event].filter(cb => cb !== callback);
        };
    }

    _emit(event, data) {
        const cbs = this.listeners[event];
        if (cbs) cbs.forEach(cb => cb(data));
    }

    disconnect() {
        this._closed = true;
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        if (this.es) {
            this.es.close();
            this.es = null;
        }
        this.connected = false;
    }
}
