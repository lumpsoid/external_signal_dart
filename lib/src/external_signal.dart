import 'dart:async';

/// {@template external_signal_listener}
/// A lifecycle-aware handler for a single external signal source.
///
/// An external signal is anything that originates outside the application's
/// own control flow — a platform channel, a push notification, a hardware
/// sensor, a timer, another feature's action stream, and so on.
///
/// Implementations subscribe to their signal source in [onAttach] and release
/// all acquired resources in [onDetach]. The two methods form a strict pair:
/// [onDetach] is always called after [onAttach], even when attachment was
/// cancelled mid-flight, so resource cleanup is always guaranteed.
///
/// Implementations should be stateless except for the subscription handles
/// they manage internally between [onAttach] and [onDetach].
///
/// Lifecycle contract:
/// ```
/// onAttach  →  (signal arrives, signal arrives, …)  →  onDetach
/// ```
/// A new attach/detach cycle may begin after [onDetach] returns.
/// {@endtemplate}
abstract class ExternalSignalListener {
  /// {@macro external_signal_listener}
  const ExternalSignalListener();

  /// Called when the owner attaches and this listener should begin
  /// receiving signals.
  ///
  /// Subscribe to the signal source here and hold any handles needed
  /// for cleanup in [onDetach].
  ///
  /// [cancellationToken] indicates that the owner has detached before
  /// this method completed. Use it to abort expensive or multi-step
  /// setup early:
  ///
  /// **Await-based (recommended):**
  /// ```dart
  /// await Future.any([expensiveSetup(), cancellationToken.signal]);
  /// if (cancellationToken.isCancelled) return;
  /// ```
  ///
  /// **Poll-based (inside loops):**
  /// ```dart
  /// for (final step in steps) {
  ///   if (cancellationToken.isCancelled) return;
  ///   await step();
  /// }
  /// ```
  ///
  /// [onDetach] will always be called after this method returns or
  /// completes, regardless of whether the token was cancelled.
  Future<void> onAttach(CancellationToken cancellationToken);

  /// Called when the owner detaches or when [onAttach] was cancelled.
  ///
  /// Cancel subscriptions and release every resource acquired — even
  /// partially — during [onAttach]. This method must be safe to call
  /// even if [onAttach] did not fully complete.
  Future<void> onDetach();
}

/// {@template cancellation_token}
/// A read-only cancellation signal passed to [ExternalSignalListener.onAttach].
///
/// Issued by [ExternalSignalRegistrar] when the owner detaches before
/// attachment has completed. Implementations should check [isCancelled]
/// or race against [signal] to abort expensive setup early and avoid
/// committing resources that will immediately need to be released.
///
/// Usage patterns:
///
/// **Await-based (recommended):**
/// ```dart
/// await Future.any([myHeavySetup(), token.signal]);
/// if (token.isCancelled) return;
/// ```
///
/// **Poll-based (inside loops):**
/// ```dart
/// for (final step in steps) {
///   if (token.isCancelled) return;
///   await step();
/// }
/// ```
/// {@endtemplate}
class CancellationToken {
  /// {@macro cancellation_token}
  CancellationToken() : _completer = Completer<void>();

  final Completer<void> _completer;

  /// Whether cancellation has been requested.
  bool get isCancelled => _completer.isCompleted;

  /// A [Future] that completes when cancellation is requested.
  ///
  /// Compose with [Future.any] to race your work against cancellation:
  /// ```dart
  /// await Future.any([doWork(), token.signal]);
  /// ```
  Future<void> get signal => _completer.future;

  void _cancel() {
    if (!_completer.isCompleted) _completer.complete();
  }
}

enum _Phase { idle, attaching, attached, detaching }

/// {@template external_signal_registrat}
/// Manages the attach/detach lifecycle for a fixed list of
/// [ExternalSignalListener] instances.
///
/// Guarantees:
/// - Listeners are attached concurrently and detached concurrently.
/// - A detach wave always waits for the preceding attach wave to drain
///   before proceeding, preventing partial or interleaved state.
/// - If [onDetach] is called while attachment is still in progress,
///   all in-flight [CancellationToken]s are cancelled immediately and
///   detachment begins as soon as the attach wave drains.
/// - [onDetach] is always called on every listener whose [onAttach] was
///   started, even if [onAttach] was cancelled or threw.
/// - Errors from individual listeners are forwarded to [onError] and do
///   not interrupt the rest of the wave.
///
/// Typical usage:
/// ```dart
/// final registrat = ExternalSignalRegistrar(
///   [MyListener(), AnotherListener()],
///   onError: (listener, error, st) => log.error(error, st),
/// );
///
/// // on mount
/// registrat.onAttach();
///
/// // on dispose
/// registrat.onDetach();
/// ```
/// {@endtemplate}
class ExternalSignalRegistrar {
  /// {@macro external_signal_registrat}
  ///
  /// [listeners] is the fixed set of listeners managed by this registrat.
  /// The list is copied and made unmodifiable at construction time.
  ///
  /// [onError] is called when a listener's [onAttach] or [onDetach] throws.
  /// If null, errors are silently swallowed. Either way, the wave continues
  /// for the remaining listeners.
  ExternalSignalRegistrar(
    List<ExternalSignalListener> listeners, {
    void Function(ExternalSignalListener, Object error, StackTrace)? onError,
  }) : _listeners = List.unmodifiable(listeners),
       _onError = onError;

  final List<ExternalSignalListener> _listeners;
  final void Function(ExternalSignalListener, Object, StackTrace)? _onError;

  _Phase _phase = _Phase.idle;

  final List<CancellationToken> _tokens = [];
  Completer<void>? _attachCompleter;
  Completer<void>? _detachCompleter;

  /// Starts a new attach cycle, calling [ExternalSignalListener.onAttach]
  /// on all listeners concurrently.
  ///
  /// If a detach wave from a previous cycle is still draining, attachment
  /// waits for it to complete before starting.
  ///
  /// No-op if already attaching or attached.
  void onAttach() {
    if (_phase != _Phase.idle) return;
    _phase = _Phase.attaching;
    _attachCompleter = Completer<void>();
    unawaited(_runAttachWave());
  }

  /// Cancels any in-flight attachments and starts a detach wave, calling
  /// [ExternalSignalListener.onDetach] on all listeners whose [onAttach]
  /// was started.
  ///
  /// If attachment is still in progress, all issued [CancellationToken]s
  /// are cancelled immediately. The detach wave begins only after the
  /// attach wave has fully drained.
  ///
  /// No-op if idle or already detaching.
  void onDetach() {
    if (_phase != _Phase.attaching && _phase != _Phase.attached) return;
    _phase = _Phase.detaching;
    _detachCompleter = Completer<void>();
    for (final token in _tokens) {
      token._cancel();
    }
    unawaited(_runDetachWave());
  }

  Future<void> _runAttachWave() async {
    if (_detachCompleter != null) {
      await _detachCompleter!.future;
    }
    await Future.wait(_listeners.map(_attachOne));
    if (_phase == _Phase.attaching) _phase = _Phase.attached;
    _attachCompleter!.complete();
  }

  Future<void> _runDetachWave() async {
    await _attachCompleter!.future;
    await Future.wait(_listeners.take(_tokens.length).map(_detachOne));
    _tokens.clear();
    _attachCompleter = null;
    _detachCompleter!.complete();
    _detachCompleter = null;
    _phase = _Phase.idle;
  }

  Future<void> _attachOne(ExternalSignalListener listener) async {
    final token = CancellationToken();
    _tokens.add(token);
    try {
      await listener.onAttach(token);
    } on Exception catch (e, st) {
      _onError?.call(listener, e, st);
    }
  }

  Future<void> _detachOne(ExternalSignalListener listener) async {
    try {
      await listener.onDetach();
    } on Exception catch (e, st) {
      _onError?.call(listener, e, st);
    }
  }
}
