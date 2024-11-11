import { Transform, TransformCallback, TransformOptions } from 'stream';
import { EventEmitter } from 'events';

export interface Progress {
    percentage: number;
    transferred: number;
    total: number;
    remaining: number;
    eta: number;
    runtime: number;
    delta: number;
    speed: number;
}

interface SpeedMonitorOptionsRequired {
    total: number;
    interval: number;
}

interface SpeedMonitorOptionsOptional {
    transferred: number;
    windowSize: number;
    autoStart: boolean;
}

export type SpeedMonitorOptions = SpeedMonitorOptionsRequired & Partial<SpeedMonitorOptionsOptional>;

interface SpeedMonitorEvents {
    'progress': (progress: Progress) => void;
}

export declare interface SpeedMonitor {
    addListener<U extends keyof SpeedMonitorEvents>(
        event: U,
        listener: SpeedMonitorEvents[U],
    ): this;

    emit<U extends keyof SpeedMonitorEvents>(
        event: U,
        ...args: Parameters<SpeedMonitorEvents[U]>,
    ): boolean;

    on<U extends keyof SpeedMonitorEvents>(
        event: U,
        listener: SpeedMonitorEvents[U],
    ): this;

    once<U extends keyof SpeedMonitorEvents>(
        event: U,
        listener: SpeedMonitorEvents[U],
    ): this;

    prependListener<U extends keyof SpeedMonitorEvents>(
        event: U,
        listener: SpeedMonitorEvents[U],
    ): this;

    prependOnceListener<U extends keyof SpeedMonitorEvents>(
        event: U,
        listener: SpeedMonitorEvents[U],
    ): this;

    removeListener<U extends keyof SpeedMonitorEvents>(
        event: U,
        listener: SpeedMonitorEvents[U],
    ): this;
}

const DEFAULT_SPEED_MONITOR_OPTIONS: SpeedMonitorOptionsOptional = {
    transferred: 0,
    windowSize: 4,
    autoStart: true,
};

export class SpeedMonitor extends EventEmitter {
    readonly total: number;
    readonly interval: number;
    readonly startTime: number;
    readonly windowSize: number;

    private _speed: number;
    private _transferred: number;
    private readonly speedBuffer: number[];
    private speedBufferPointer: number;
    private lastLoaded: number;
    private lastTime: number;
    private timer?: number;

    constructor(options: SpeedMonitorOptions) {
        const _options: SpeedMonitorOptionsRequired & SpeedMonitorOptionsOptional = {
            ...DEFAULT_SPEED_MONITOR_OPTIONS,
            ...options,
        };

        const {
            transferred,
            windowSize,
            total,
            interval,
            autoStart,
            ...eventEmitterOptions
        } = _options;

        super(eventEmitterOptions);

        this.total = total;
        this.startTime = Date.now();
        this._speed = 0;
        this._transferred = transferred;
        this.windowSize = windowSize;

        this.speedBuffer = [];
        this.speedBufferPointer = 0;
        this.lastLoaded = this.transferred;
        this.lastTime = this.startTime;
        this.interval = interval;

        this.handleProgress = this.handleProgress.bind(this);
        if (autoStart) {
            this.start();
        }

        this.destroy = this.destroy.bind(this);
    }

    get paused(): boolean {
        return !this.timer;
    }

    get running(): boolean {
        return !this.paused;
    }

    /**
     * Bytes/ms
     */
    get speed(): number {
        return this._speed;
    }

    get transferred(): number {
        return this._transferred;
    }

    updateProgress(delta: number) {
        this._transferred += delta;
        if (delta < 0) {
            this.lastLoaded += delta;
        }
    }

    start() {
        if (this.running) {
            return;
        }
        this.timer = setInterval(this.handleProgress, this.interval) as unknown as number;
    }

    pause() {
        if (this.paused) {
            return;
        }
        if (this.lastLoaded !== this._transferred) {
            this.handleProgress();
        }
        this.clearTimer();
    }

    private clearTimer() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = undefined;
        }
    }

    private calcSpeed(currentSpeed: number): number {
        if (this.speedBuffer.length < this.windowSize) {
            this.speedBuffer.push(currentSpeed);
        } else {
            this.speedBuffer[this.speedBufferPointer] = currentSpeed;
        }
        this.speedBufferPointer = (this.speedBufferPointer + 1) % this.windowSize;

        return this.speedBuffer.reduce(
            (avg, curr, idx) => avg + ((curr - avg) / (idx + 1))
        );
    }

    private handleProgress() {
        const now = Date.now();
        const delta = this.transferred - this.lastLoaded;
        const duration = now - this.lastTime;
        this.lastLoaded = this.transferred;
        this.lastTime = now;

        this._speed = this.calcSpeed(delta / duration);

        const remaining = this.total - this.transferred;
        const eta = remaining / this._speed;

        const progress: Progress = {
            delta: delta,
            eta: eta,
            percentage: Math.min(100, this.transferred * 100 / this.total),
            remaining: remaining,
            runtime: now - this.startTime,
            speed: this._speed,
            total: this.total,
            transferred: this.transferred,
        };

        this.emit('progress', progress);
    }

    destroy() {
        this.pause();
        this.removeAllListeners();
    }
}

export interface ProgressOptions extends TransformOptions {
    monitor: SpeedMonitor;
}

export class ProgressStream extends Transform {
    private readonly monitor: SpeedMonitor;
    private readonly customTransform?: TransformOptions['transform'];

    constructor(options: ProgressOptions) {
        const {
            monitor,
            transform,
            ...transformOptions
        } = options;
        super(transformOptions);
        this.monitor = monitor;
        this.customTransform = transform;

        this.on('pipe', () => this.monitor.start());
        this.on('unpipe', () => this.monitor.pause());
        this.on('close', () => this.monitor.destroy());
        this.on('error', () => this.monitor.destroy());
    }

    _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback) {
        this.monitor.updateProgress(chunk.length);
        if (this.customTransform) {
            this.customTransform(chunk, encoding, callback);
        } else {
            callback(null, chunk);
        }
    }
}
