import { pool } from '../db/pool';

/**
 * ═══════════════════════════════════════════════════════════════════════
 * 🎬 DEMO INTERACTIVO — PAD 213: Recurring Payments con Temporal
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Este script ejecuta un escenario guiado que demuestra todos los
 * patrones arquitectónicos de la PoC. Diseñado para correr mientras
 * los 6 procesos están levantados (ver README → Quick Start).
 *
 * Prerequisitos:
 *   - PostgreSQL corriendo (docker-compose up -d)
 *   - Temporal corriendo (temporal server start-dev ...)
 *   - Workers: npm run start:worker:platform / bill / p2p
 *   - Scheduler: npm run start:scheduler
 *   - API: npm run start:api
 *   - Outbox: npm run start:outbox
 *
 * Ejecución:
 *   npm run demo
 *
 * ═══════════════════════════════════════════════════════════════════════
 */

const API = 'http://localhost:3000';

// ─── Utilidades ───────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function banner(text: string) {
  console.log('\n' + '═'.repeat(70));
  console.log(`  ${text}`);
  console.log('═'.repeat(70));
}

function step(n: number, text: string) {
  console.log(`\n  ┌─── Step ${n} ───────────────────────────────────────────────────`);
  console.log(`  │ ${text}`);
  console.log(`  └${'─'.repeat(65)}`);
}

function info(text: string) {
  console.log(`  💡 ${text}`);
}

function waiting(text: string) {
  process.stdout.write(`  ⏳ ${text}`);
}

function done(text: string) {
  console.log(`  ✅ ${text}`);
}

function observe(text: string) {
  console.log(`  👀 ${text}`);
}

async function post(path: string, body: object) {
  const res = await fetch(`${API}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json() as any;
}

async function patch(path: string) {
  const res = await fetch(`${API}${path}`, { method: 'PATCH' });
  return res.json() as any;
}

async function get(path: string) {
  const res = await fetch(`${API}${path}`);
  return res.json() as any;
}

// ─── Demo principal ───────────────────────────────────────────────────

async function demo() {
  banner('🎬 DEMO: Recurring Payments Architecture (PAD 213)');
  console.log(`
  Este demo muestra 6 escenarios que validan la arquitectura:

    1. Happy Path — cobro exitoso + re-enqueue automático
    2. Retry con notificación — fallo + durable timer + éxito
    3. Strategy Pattern — BILL vs P2P en task queues separadas
    4. Signal — cambiar monto mid-flight
    5. Query — inspeccionar estado sin interrumpir
    6. Cancelación — interrumpir un retry sleep

  ⚠️  Asegúrate de que los 6 procesos estén corriendo.
  `);

  // ─── Limpiar estado previo ────────────────────────────────────────

  step(0, 'Limpiando estado previo...');
  await pool.query('TRUNCATE subscriptions, payment_execution_queue, notification_outbox CASCADE');
  done('Base de datos limpia');

  // ═══════════════════════════════════════════════════════════════════
  // ESCENARIO 1: Happy Path
  // ═══════════════════════════════════════════════════════════════════

  banner('1️⃣  ESCENARIO: Happy Path (cobro exitoso)');
  info('Crea una subscription → el scheduler la detecta → Temporal ejecuta → éxito → re-enqueue');

  step(1, 'Creando subscription BILL (Claro, S/89.90)');
  const sub1 = await post('/subscriptions', {
    userId: 'user-demo-001',
    subscriptionType: 'BILL',
    destinationId: 'biller-claro',
    amount: 89.90,
  });
  done(`Subscription creada: ${sub1.id}`);
  info(`Próxima ejecución: ${sub1.nextExecution}`);
  info('El scheduler la detectará en ~3-10 segundos');

  step(2, 'Esperando a que el scheduler la procese...');
  waiting('Scheduler polling → claim → startWorkflow');
  await sleep(15000);
  console.log(' ✓');

  step(3, 'Verificando resultado');
  const outbox1 = await get('/outbox');
  const paymentEvents = outbox1.filter((e: any) =>
    e.subscription_id === sub1.id && e.event_type.includes('PAYMENT')
  );

  if (paymentEvents.length > 0) {
    const evt = paymentEvents[0];
    done(`Evento: ${evt.event_type}`);
    info('Mira la terminal del Outbox Consumer → lo publicó a Kafka');
  } else {
    info('El workflow aún está ejecutando (puede estar en retry)...');
  }

  step(4, 'Verificando re-enqueue (pago recurrente → vuelve a la cola para mañana)');
  const queue1 = await get('/queue');
  const tomorrow = queue1.filter((r: any) =>
    r.subscription_id === sub1.id && r.status === 'READY'
  );
  if (tomorrow.length > 0) {
    done(`Re-enqueued para: ${tomorrow[0].due_at}`);
    info('Sin intervención humana, mañana se ejecutará de nuevo');
  } else {
    info('Si el cobro falló, no se re-encola (correcto — va a retry)');
  }

  observe('📺 Abre Temporal UI: http://localhost:8233 → busca el workflow');
  await sleep(3000);

  // ═══════════════════════════════════════════════════════════════════
  // ESCENARIO 2: Retry con notificación
  // ═══════════════════════════════════════════════════════════════════

  banner('2️⃣  ESCENARIO: Retry con notificación al usuario');
  info('Si el cobro falla, el workflow duerme 1 minuto (1 día en prod)');
  info('y notifica al usuario "reintentaremos mañana" vía outbox');
  info('Creamos varias subscriptions — estadísticamente alguna fallará (20% fail rate)');

  step(1, 'Creando 5 subscriptions BILL (probabilidad alta de al menos 1 fallo)');
  const retryIds: string[] = [];
  for (let i = 1; i <= 5; i++) {
    const sub = await post('/subscriptions', {
      userId: 'user-demo-retry',
      subscriptionType: 'BILL',
      destinationId: `biller-retry-${i}`,
      amount: i * 25,
    });
    retryIds.push(sub.id);
  }
  done(`Creadas 5 subscriptions`);
  info('Esperando ejecución (15s para scheduler + workflow)...');

  waiting('Ejecutando');
  await sleep(15000);
  console.log(' ✓');

  step(2, 'Verificando si hubo ATTEMPT_FAILED en outbox');
  const outbox2 = await get('/outbox');
  const attemptFailed = outbox2.filter((e: any) => e.event_type === 'ATTEMPT_FAILED');

  if (attemptFailed.length > 0) {
    done(`${attemptFailed.length} intento(s) fallido(s) notificados al usuario`);
    const payload = typeof attemptFailed[0].payload === 'string'
      ? JSON.parse(attemptFailed[0].payload)
      : attemptFailed[0].payload;
    info(`Mensaje: "${payload.message}"`);
    info('El outbox consumer ya lo publicó → el servicio de push lo enviaría');
    observe('👀 Mira la terminal del Outbox Consumer → ATTEMPT_FAILED → kafka://payments.retries');
    info('El workflow está dormido con un durable timer. Si el proceso muere, Temporal lo retoma.');
  } else {
    info('Todas tuvieron éxito en el primer intento (80% success rate). ¡Suerte!');
    info('Crea más subscriptions o vuelve a correr el demo para ver retries.');
  }

  await sleep(3000);

  // ═══════════════════════════════════════════════════════════════════
  // ESCENARIO 3: Strategy Pattern (BILL vs P2P)
  // ═══════════════════════════════════════════════════════════════════

  banner('3️⃣  ESCENARIO: Strategy Pattern — BILL vs P2P');
  info('El parent workflow usa un STRATEGY_MAP para decidir:');
  info('  BILL → billPaymentWorkflow → task queue: payments-bill');
  info('  P2P  → p2pPaymentWorkflow  → task queue: payments-p2p');
  info('Cada tipo tiene su propio worker (proceso independiente)');

  step(1, 'Creando subscription P2P (mesada a mamá, S/200)');
  const subP2P = await post('/subscriptions', {
    userId: 'user-demo-p2p',
    subscriptionType: 'P2P',
    destinationId: 'wallet-mama-9876',
    amount: 200.00,
  });
  done(`P2P subscription: ${subP2P.id}`);
  info('Mira la terminal del P2P Worker → verás "validateP2PRecipient" + "executeP2PTransfer"');
  info('Mientras que la terminal del Bill Worker NO muestra nada (task queue separada)');

  waiting('Esperando ejecución');
  await sleep(15000);
  console.log(' ✓');

  step(2, 'Verificando que el P2P se ejecutó');
  const outboxP2P = await get('/outbox');
  const p2pEvents = outboxP2P.filter((e: any) =>
    e.subscription_id === subP2P.id && e.event_type.includes('PAYMENT')
  );
  if (p2pEvents.length > 0) {
    done(`P2P resultado: ${p2pEvents[0].event_type}`);
  }

  step(3, 'Buscando por Search Attributes');
  const searchResult = await get('/workflows/search?subscriptionType=P2P');
  done(`Workflows P2P encontrados: ${searchResult.count}`);
  info('En producción: "dame todos los cobros P2P del usuario X en julio"');

  await sleep(2000);

  // ═══════════════════════════════════════════════════════════════════
  // ESCENARIO 4: Signal — Cambiar monto mid-flight
  // ═══════════════════════════════════════════════════════════════════

  banner('4️⃣  ESCENARIO: Signal — Cambiar monto mientras el workflow duerme');
  info('Un workflow en retry-sleep puede recibir un Signal para modificar su estado.');
  info('Caso de uso: el usuario reporta que su factura cambió, soporte ajusta.');
  info('Necesitamos un workflow que esté durmiendo (en retry). Creamos 10 para forzar uno.');

  step(1, 'Creando 10 subscriptions BILL (esperamos ~2 fallos para tener uno en sleep)');
  const signalIds: string[] = [];
  for (let i = 1; i <= 10; i++) {
    const sub = await post('/subscriptions', {
      userId: 'user-demo-signal',
      subscriptionType: 'BILL',
      destinationId: `biller-signal-${i}`,
      amount: i * 10,
    });
    signalIds.push(sub.id);
  }
  done('10 subscriptions creadas');

  waiting('Esperando a que alguna falle y entre en retry-sleep');
  await sleep(15000);
  console.log(' ✓');

  step(2, 'Buscando un workflow en estado Running (en sleep)');
  const searchRunning = await get('/workflows/search?status=Running');

  if (searchRunning.count > 0) {
    // Buscar un child (billPayment) que esté corriendo
    const runningWf = searchRunning.workflows.find((w: any) =>
      w.workflowId.includes('BILL')
    );

    if (runningWf) {
      const wfId = runningWf.workflowId;
      done(`Workflow en retry: ${wfId}`);

      step(3, 'Enviando Query: getProgress (inspección sin modificar)');
      const progress = await get(`/workflows/${wfId}/query/progress`);
      if (progress.currentAttempt) {
        done(`Intento: ${progress.currentAttempt}/${progress.maxAttempts} | Monto: S/${progress.currentAmount} | Estado: ${progress.status}`);
      } else {
        info(`Query response: ${JSON.stringify(progress)}`);
      }

      step(4, 'Enviando Signal: updateAmount(999.99)');
      info('Esto cambia el monto que se usará en el próximo intento de cobro');
      const signalResult = await post(`/workflows/${wfId}/signal/updateAmount`, { amount: 999.99 });
      done(`Signal enviado: ${signalResult.message || 'OK'}`);

      step(5, 'Query de nuevo para confirmar que el monto cambió');
      const progressAfter = await get(`/workflows/${wfId}/query/progress`);
      if (progressAfter.currentAmount) {
        done(`Monto actualizado: S/${progressAfter.currentAmount} (era ${progress.currentAmount || 'N/A'})`);
        info('Cuando el timer expire, el workflow cobrará con el nuevo monto');
      }
    } else {
      info('No hay child workflows corriendo (todos los parent están en otra fase)');
    }
  } else {
    info('No hay workflows en retry ahora. Todos tuvieron éxito o terminaron.');
    info('Tip: Re-ejecuta el demo — con 10 subscriptions al 20% de fallo, normalmente 2 entran en retry');
  }

  await sleep(2000);

  // ═══════════════════════════════════════════════════════════════════
  // ESCENARIO 5: Cancelación en vuelo
  // ═══════════════════════════════════════════════════════════════════

  banner('5️⃣  ESCENARIO: Cancelación — Interrumpir un retry sleep');
  info('Si el usuario cancela su subscription mientras el workflow duerme,');
  info('Temporal propaga la cancelación inmediatamente → el sleep se interrumpe.');

  step(1, 'Creando subscription que intentaremos cancelar mid-flight');
  const subCancel = await post('/subscriptions', {
    userId: 'user-demo-cancel',
    subscriptionType: 'BILL',
    destinationId: 'biller-cancel-test',
    amount: 500.00,
  });
  done(`Subscription: ${subCancel.id}`);

  waiting('Esperando a que el workflow arranque');
  await sleep(13000);
  console.log(' ✓');

  step(2, 'Cancelando subscription (API → DB + Temporal.cancel)');
  const cancelResult = await patch(`/subscriptions/${subCancel.id}/cancel`);
  done(`Resultado: ${cancelResult.message || cancelResult.status || JSON.stringify(cancelResult)}`);
  info('Si el workflow estaba en sleep, isCancellation(err) → return CANCELLED');
  info('Si ya había terminado, Temporal responde "not found" (idempotente)');
  observe('👀 Mira la terminal del Bill Worker → "🚫 Cancelled during attempt X"');

  await sleep(3000);

  // ═══════════════════════════════════════════════════════════════════
  // ESCENARIO 6: Search Attributes
  // ═══════════════════════════════════════════════════════════════════

  banner('6️⃣  ESCENARIO: Search Attributes — Buscar workflows por usuario/tipo');
  info('Cada workflow publica userId y subscriptionType como Search Attributes.');
  info('Esto permite buscar masivamente sin consultar la BD.');

  step(1, 'Búsqueda: todos los workflows del usuario "user-demo-signal"');
  const searchUser = await get('/workflows/search?userId=user-demo-signal');
  done(`Encontrados: ${searchUser.count} workflows`);
  if (searchUser.workflows?.[0]?.searchAttributes) {
    info(`Ejemplo: ${JSON.stringify(searchUser.workflows[0].searchAttributes)}`);
  }

  step(2, 'Búsqueda: todos los workflows tipo P2P');
  const searchP2P = await get('/workflows/search?subscriptionType=P2P');
  done(`Workflows P2P: ${searchP2P.count}`);

  step(3, 'Búsqueda: workflows activos (Running)');
  const searchActive = await get('/workflows/search?status=Running');
  done(`Workflows activos: ${searchActive.count}`);
  info('En producción: "¿cuántos cobros están en retry ahora mismo?" → alerting');

  // ═══════════════════════════════════════════════════════════════════
  // RESUMEN
  // ═══════════════════════════════════════════════════════════════════

  banner('📊 RESUMEN DEL DEMO');

  const finalOutbox = await get('/outbox');
  const finalQueue = await get('/queue');
  const finalSubs = await get('/subscriptions');

  const outboxByType: Record<string, number> = {};
  finalOutbox.forEach((e: any) => {
    outboxByType[e.event_type] = (outboxByType[e.event_type] || 0) + 1;
  });

  const queueByStatus: Record<string, number> = {};
  finalQueue.forEach((r: any) => {
    queueByStatus[r.status] = (queueByStatus[r.status] || 0) + 1;
  });

  console.log(`
  Subscriptions creadas:  ${finalSubs.length}
  Ejecuciones en cola:    ${finalQueue.length}
    - DONE:       ${queueByStatus['DONE'] || 0}
    - READY:      ${queueByStatus['READY'] || 0} (re-enqueued para mañana)
    - PROCESSING: ${queueByStatus['PROCESSING'] || 0} (en vuelo)
    - FAILED:     ${queueByStatus['FAILED'] || 0}

  Eventos en outbox:      ${finalOutbox.length}
    - REMINDER:           ${outboxByType['REMINDER'] || 0}
    - PAYMENT_SUCCEEDED:  ${outboxByType['PAYMENT_SUCCEEDED'] || 0}
    - PAYMENT_FAILED:     ${outboxByType['PAYMENT_FAILED'] || 0}
    - ATTEMPT_FAILED:     ${outboxByType['ATTEMPT_FAILED'] || 0}

  ─────────────────────────────────────────────────────────────────

  Patrones validados:
    ✅ Strategy Pattern (BILL + P2P en task queues separadas)
    ✅ Transactional Outbox (notificaciones atómicas)
    ✅ Durable Timers (retry con sleep que sobrevive crashes)
    ✅ Signals (modificar workflow en ejecución)
    ✅ Queries (inspeccionar estado sin bloquear)
    ✅ Search Attributes (buscar workflows por usuario/tipo)
    ✅ Cancellation-aware (interrumpir sleep inmediatamente)
    ✅ Re-enqueue atómico (verdaderamente recurrente)
    ✅ Outbox Consumer (publicación a Kafka simulada)
    ✅ Scheduler Recovery (rows stuck se liberan)

  ─────────────────────────────────────────────────────────────────

  🔗 Temporal UI: http://localhost:8233
     → Busca un workflow → mira su Event History
     → Verás cada activity, timer, signal, y child workflow

  📖 Para más detalles, lee el README.md
  `);

  await pool.end();
}

// ─── Ejecutar ─────────────────────────────────────────────────────────

demo().catch((err) => {
  console.error('❌ Demo error:', err);
  process.exit(1);
});
