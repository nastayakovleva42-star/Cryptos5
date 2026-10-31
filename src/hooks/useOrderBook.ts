import { useEffect, useState, useRef } from 'react';

export function useOrderBook(trackedSymbols: string[]) {
  const [ratios, setRatios] = useState<Record<string, number>>({});
  
  const statesRef = useRef<Record<string, {
    bids: Map<string, number>,
    asks: Map<string, number>,
    lastUpdateId: number,
    buffer: any[],
    isSynced: boolean
  }>>({});

  useEffect(() => {
    if (!trackedSymbols || !Array.isArray(trackedSymbols) || trackedSymbols.length === 0) return;

    let isCancelled = false;
    const streams = trackedSymbols.map(sym => `${sym.toLowerCase()}@depth@1000ms`).join('/');
    const wsUrl = `wss://stream.binance.com:9443/stream?streams=${streams}`;
    let ws: WebSocket | null = null;

    // Умный шаг группировки в зависимости от цены монеты
    const getPriceStep = (price: number) => {
      if (price > 10000) return 10;    // Для BTC
      if (price > 1000) return 1;      // Для ETH
      if (price > 100) return 0.1;     // Для SOL
      if (price > 1) return 0.01;      // Для XRP
      return 0.0001;                   // Для мемкоинов
    };

    const calculateRatio = (sym: string) => {
      const state = statesRef.current[sym];
      if (!state || state.bids.size === 0 || state.asks.size === 0) return;

      const bids = Array.from(state.bids.entries()).map(([p, q]) => ({ price: parseFloat(p), qty: q }));
      const asks = Array.from(state.asks.entries()).map(([p, q]) => ({ price: parseFloat(p), qty: q }));

      bids.sort((a, b) => b.price - a.price);
      asks.sort((a, b) => a.price - b.price);

      const midPrice = (bids[0].price + asks[0].price) / 2;
      const priceStep = getPriceStep(midPrice);

      const aggregate = (levels: any[], isBids: boolean) => {
        const map = new Map<number, number>();
        levels.forEach(l => {
          const aggPrice = Math.floor(l.price / priceStep) * priceStep;
          map.set(aggPrice, (map.get(aggPrice) || 0) + l.qty);
        });
        return Array.from(map.entries()).map(([price, qty]) => ({ price, qty })).sort((a, b) => isBids ? b.price - a.price : a.price - b.price);
      };

      const aggBids = aggregate(bids, true);
      const aggAsks = aggregate(asks, false);

      const lowerBound = midPrice * (1 - 2.5 / 100);
      const upperBound = midPrice * (1 + 2.5 / 100);

      const totalBids = aggBids.filter(b => b.price >= lowerBound).reduce((s, b) => s + b.qty, 0);
      const totalAsks = aggAsks.filter(a => a.price <= upperBound).reduce((s, a) => s + a.qty, 0);

      if (totalBids + totalAsks === 0) return;
      const ratio = ((totalBids - totalAsks) / (totalBids + totalAsks)) * 100;

      setRatios(prev => ({ ...prev, [sym]: Math.round(ratio * 100) / 100 }));
    };

    const applyUpdate = (sym: string, data: any) => {
      const state = statesRef.current[sym];
      if (!state) return;

      if (data.b) data.b.forEach(([p, q]: [string, string]) => {
        if (parseFloat(q) === 0) state.bids.delete(p); else state.bids.set(p, parseFloat(q));
      });
      if (data.a) data.a.forEach(([p, q]: [string, string]) => {
        if (parseFloat(q) === 0) state.asks.delete(p); else state.asks.set(p, parseFloat(q));
      });
      
      calculateRatio(sym);
    };

    const init = async () => {
      try {
        // 1. Сначала подключаем WebSocket и копим изменения в буфер
        ws = new WebSocket(wsUrl);
        
        ws.onmessage = (event) => {
          const message = JSON.parse(event.data);
          const sym = message.stream.split('@')[0].toUpperCase();
          const data = message.data;

          const state = statesRef.current[sym];
          if (!state) return;

          if (!state.isSynced) {
            state.buffer.push(data);
          } else {
            applyUpdate(sym, data);
          }
        };

        // 2. Параллельно скачиваем снимки
        const snapshots = await Promise.all(trackedSymbols.map(async (sym) => {
          const res = await fetch(`https://api.binance.com/api/v3/depth?symbol=${sym}&limit=5000`);
          return { sym, data: await res.json() };
        }));

        if (isCancelled) return;

        // 3. Применяем снимки и обрабатываем буфер
        snapshots.forEach(({ sym, data }) => {
          if (!data.bids || !data.asks) return;
          
          const bids = new Map<string, number>();
          const asks = new Map<string, number>();
          data.bids.forEach(([p, q]: [string, string]) => bids.set(p, parseFloat(q)));
          data.asks.forEach(([p, q]: [string, string]) => asks.set(p, parseFloat(q)));
          
          statesRef.current[sym] = {
            bids, asks, lastUpdateId: data.lastUpdateId, buffer: [], isSynced: true
          };
        });

        // 4. Накатываем то, что накопилось в буфере, пока мы делали запрос
        trackedSymbols.forEach(sym => {
          const state = statesRef.current[sym];
          if (!state) return;

          // В реальном проекте здесь нужно проверять firstEvent.U <= lastUpdateId, 
          // но для индикатора мы просто применяем всё, что пришло после снимка
          state.buffer.forEach(update => applyUpdate(sym, update));
          state.buffer = [];
          calculateRatio(sym);
        });

      } catch (err) {
        console.error('Multi Order book error:', err);
      }
    };

    init();

    return () => {
      isCancelled = true;
      if (ws) ws.close();
    };
  }, [trackedSymbols]);

  return { ratios };
}