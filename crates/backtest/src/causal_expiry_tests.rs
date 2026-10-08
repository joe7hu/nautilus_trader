mod causal_remaining {
    use super::*;
    use nautilus_common::messages::execution::CancelOrder;
    use nautilus_execution::models::fee::{FeeModelHandle, FixedFeeModel};
    use nautilus_model::{
        enums::{ContingencyType, TimeInForce},
        identifiers::Symbol,
        instruments::CurrencyPair,
        types::Currency,
    };

    fn engine() -> BacktestEngine {
        BacktestEngine::new(BacktestEngineConfig {
            bypass_logging: true,
            run_analysis: false,
            ..Default::default()
        })
        .unwrap()
    }
    fn venue(e: &mut BacktestEngine, id: &str, causal: bool, gtd: bool) {
        e.add_venue(
            SimulatedVenueConfig::builder()
                .venue(Venue::from(id))
                .oms_type(OmsType::Netting)
                .account_type(AccountType::Cash)
                .book_type(BookType::L1_MBP)
                .starting_balances(vec![Money::from("1000.00 USD")])
                .fee_model(FeeModelHandle::new(
                    FixedFeeModel::new(Money::from("0.25 USD"), Some(false)).unwrap(),
                ))
                .liquidity_consumption(true)
                .gtd_expiry_before_match(causal)
                .support_gtd_orders(gtd)
                .build()
                .unwrap(),
        )
        .unwrap();
        e.venues
            .get(&Venue::from(id))
            .unwrap()
            .borrow_mut()
            .initialize_account();
    }
    fn instrument(e: &mut BacktestEngine, id: &str) -> InstrumentId {
        let id = InstrumentId::from(id);
        let i = CurrencyPair::builder()
            .instrument_id(id)
            .raw_symbol(Symbol::from(id.symbol.as_str()))
            .base_currency(Currency::EUR())
            .quote_currency(Currency::USD())
            .price_precision(5)
            .size_precision(0)
            .price_increment(Price::from("0.00001"))
            .size_increment(Quantity::from("1"))
            .ts_event(1.into())
            .ts_init(1.into())
            .build()
            .unwrap();
        e.add_instrument(&InstrumentAny::CurrencyPair(i)).unwrap();
        quote(id, 1, "1.10000", 10);
        id
    }
    fn quote(id: InstrumentId, at: u64, ask: &str, size: u64) {
        let q = QuoteTick::new(
            id,
            Price::from("0.90000"),
            Price::from(ask),
            Quantity::from(size),
            Quantity::from(size),
            at.into(),
            at.into(),
        );
        msgbus::send_quote(
            format!("SimulatedExchange.process_new_quote.{}", id.venue).into(),
            &q,
        );
    }
    fn order(e: &BacktestEngine, id: InstrumentId, name: &str, expiry: Option<u64>) -> OrderAny {
        let mut b = OrderTestBuilder::new(OrderType::Limit);
        b.trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(ClientOrderId::from(name))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("1.00000"));
        if let Some(at) = expiry {
            b.time_in_force(TimeInForce::Gtd).expire_time(at.into());
        }
        b.build()
    }
    fn cache(e: &BacktestEngine, o: &OrderAny) {
        e.kernel
            .cache
            .borrow_mut()
            .add_order(
                o.clone(),
                None,
                Some(ClientId::from(o.instrument_id().venue.as_str())),
                false,
            )
            .unwrap();
    }
    fn submit(o: &OrderAny, at: u64) {
        send_execution_command(TradingCommand::SubmitOrder(SubmitOrder::new(
            o.trader_id(),
            Some(ClientId::from(o.instrument_id().venue.as_str())),
            o.strategy_id(),
            o.instrument_id(),
            o.client_order_id(),
            o.init_event().clone(),
            o.exec_algorithm_id(),
            None,
            None,
            UUID4::new(),
            at.into(),
            None,
        )));
    }
    fn setup(causal: bool, gtd: bool, expiry: Option<u64>) -> (BacktestEngine, OrderAny) {
        let mut e = engine();
        venue(&mut e, "CA", causal, gtd);
        let i = instrument(&mut e, "EUR/USD.CA");
        let o = order(&e, i, "ORIGINAL", expiry);
        cache(&e, &o);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(2.into(), &clocks).unwrap();
        submit(&o, 2);
        e.settle_venues(2.into(), SettlementScope::All);
        (e, o)
    }
    fn snapshot(e: &BacktestEngine, o: &OrderAny) -> OrderAny {
        e.kernel
            .cache
            .borrow()
            .order(&o.client_order_id())
            .unwrap()
            .clone()
    }
    fn economics(o: &OrderAny, qty: u64) {
        let fills: Vec<_> = o
            .events()
            .iter()
            .filter_map(|event| {
                if let OrderEventAny::Filled(f) = event {
                    Some(f)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(o.filled_qty(), Quantity::from(qty));
        assert_eq!(fills.len(), if qty == 0 { 0 } else { 1 });
        if qty > 0 {
            assert_eq!(fills[0].last_qty, Quantity::from(8));
            assert_eq!(fills[0].last_px, Price::from("1.10000"));
            assert_eq!(fills[0].commission, Some(Money::from("0.25 USD")));
        }
    }
    fn terminal(o: &OrderAny) -> Vec<(String, u64)> {
        o.events()
            .iter()
            .filter_map(|v| match v {
                OrderEventAny::Expired(v) => Some(("Expired".into(), v.ts_event.as_u64())),
                OrderEventAny::Canceled(v) => Some(("Canceled".into(), v.ts_event.as_u64())),
                _ => None,
            })
            .collect()
    }
    fn callback(
        e: &mut BacktestEngine,
        o: &OrderAny,
        at: u64,
        action: &str,
        name: &str,
        observed: Rc<RefCell<Vec<(OrderStatus, u64)>>>,
    ) {
        let cache = e.kernel.cache.clone();
        let o = o.clone();
        let action = action.to_string();
        let cb = TimeEventCallback::RustLocal(Rc::new(move |event| {
            let before = cache.borrow().order(&o.client_order_id()).unwrap().status();
            observed
                .borrow_mut()
                .push((before, event.ts_event.as_u64()));
            if action == "cancel" {
                send_execution_command(TradingCommand::CancelOrder(CancelOrder::new(
                    o.trader_id(),
                    Some(ClientId::from(o.instrument_id().venue.as_str())),
                    o.strategy_id(),
                    o.instrument_id(),
                    o.client_order_id(),
                    None,
                    UUID4::new(),
                    event.ts_event,
                    None,
                    None,
                )));
            }
            if action == "modify" {
                send_execution_command(TradingCommand::ModifyOrder(ModifyOrder::new(
                    o.trader_id(),
                    Some(ClientId::from(o.instrument_id().venue.as_str())),
                    o.strategy_id(),
                    o.instrument_id(),
                    o.client_order_id(),
                    None,
                    None,
                    Some(Price::from("1.10000")),
                    None,
                    UUID4::new(),
                    event.ts_event,
                    None,
                    None,
                )));
            }
        }));
        e.kernel
            .clock
            .borrow_mut()
            .set_timer_ns(
                name,
                DurationNanos::new(100),
                Some(at.into()),
                None,
                Some(cb),
                Some(true),
                Some(true),
            )
            .unwrap();
    }
    #[rstest]
    fn timer_ordering(
        #[values(false, true)] causal: bool,
        #[values(4_u64, 5, 6)] at: u64,
        #[values("read", "cancel", "modify")] action: &str,
        #[values("jump", "boundary", "end")] route: &str,
    ) {
        let (mut e, o) = setup(causal, true, Some(5));
        let observed = Rc::new(RefCell::new(Vec::new()));
        callback(&mut e, &o, at, action, "SCHEDULE", observed.clone());
        let clocks = e.collect_all_clocks();
        match route {
            "jump" => {
                e.advance_time_impl(7.into(), &clocks).unwrap();
            }
            "boundary" => {
                e.advance_time_impl(at.into(), &clocks).unwrap();
                e.finalize_timestamp(&clocks, at.into(), SettlementScope::All)
                    .unwrap();
            }
            "end" => {
                e.end_ns = 7.into();
                e.end().unwrap();
            }
            _ => unreachable!(),
        }
        let result = snapshot(&e, &o);
        let expected_before = if causal && at >= 5 {
            OrderStatus::Expired
        } else {
            OrderStatus::Accepted
        };
        let qty = if action == "modify" && (!causal || at < 5) {
            8
        } else {
            0
        };
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"timer","causal":causal,"at":at,"action":action,"route":route,"before":format!("{:?}",observed.borrow()),"status":format!("{:?}",result.status()),"filled":result.filled_qty().to_string(),"terminals":terminal(&result)})
        );
        assert_eq!(
            *observed.borrow(),
            vec![(expected_before, at)],
            "expiry must precede a due opt-in timer callback"
        );
        economics(&result, qty);
        if causal && at >= 5 {
            assert_eq!(result.status(), OrderStatus::Expired);
            assert_eq!(terminal(&result), vec![("Expired".into(), at)]);
        } else if action == "cancel" {
            assert_eq!(result.status(), OrderStatus::Canceled);
        } else if action == "modify" {
            assert_eq!(result.status(), OrderStatus::Filled);
        }
    }
    #[rstest]
    fn disabled_gtd(#[values(false, true)] causal: bool, #[values(false, true)] is_gtd: bool) {
        let (mut e, o) = setup(causal, false, is_gtd.then_some(5));
        let initial = snapshot(&e, &o);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(5.into(), &clocks).unwrap();
        quote(o.instrument_id(), 5, "1.10000", 11);
        // GTC positive control crosses only after changing its original limit.
        if !is_gtd {
            send_execution_command(TradingCommand::ModifyOrder(ModifyOrder::new(
                o.trader_id(),
                Some(ClientId::from("CA")),
                o.strategy_id(),
                o.instrument_id(),
                o.client_order_id(),
                None,
                None,
                Some(Price::from("1.10000")),
                None,
                UUID4::new(),
                5.into(),
                None,
                None,
            )));
            e.settle_venues(5.into(), SettlementScope::All);
        }
        let result = snapshot(&e, &o);
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"disabled-gtd","causal":causal,"gtd":is_gtd,"initial":format!("{:?}",initial.status()),"final":format!("{:?}",result.status())})
        );
        assert_eq!(initial.status(), OrderStatus::Accepted);
        assert_eq!(
            result.status(),
            if is_gtd {
                OrderStatus::Accepted
            } else {
                OrderStatus::Filled
            }
        );
        assert!(terminal(&result).is_empty());
        economics(&result, if is_gtd { 0 } else { 8 });
    }
    #[test]
    fn mixed_venues_independent_expiry_and_repeated_maintenance() {
        let mut e = engine();
        venue(&mut e, "CA", true, true);
        venue(&mut e, "LEGACY", false, true);
        let early_id = instrument(&mut e, "EUR/USDA.CA");
        let late_id = instrument(&mut e, "EUR/USDB.CA");
        let legacy_id = instrument(&mut e, "EUR/USD.LEGACY");
        let early = order(&e, early_id, "EARLY", Some(5));
        let late = order(&e, late_id, "LATE", Some(9));
        let legacy = order(&e, legacy_id, "LEGACY", Some(5));
        for o in [&early, &late, &legacy] {
            cache(&e, o);
            submit(o, 2);
        }
        e.settle_venues(2.into(), SettlementScope::All);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(5.into(), &clocks).unwrap();
        assert_eq!(snapshot(&e, &early).status(), OrderStatus::Expired);
        assert_eq!(snapshot(&e, &late).status(), OrderStatus::Accepted);
        assert_eq!(snapshot(&e, &legacy).status(), OrderStatus::Accepted);
        quote(early_id, 5, "1.00000", 10);
        quote(legacy_id, 5, "1.00000", 10);
        let legacy_after = snapshot(&e, &legacy);
        assert_eq!(legacy_after.status(), OrderStatus::Filled);
        assert_eq!(legacy_after.filled_qty(), Quantity::from(8));
        let f = legacy_after
            .events()
            .iter()
            .find_map(|x| {
                if let OrderEventAny::Filled(f) = x {
                    Some(f)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(f.last_px, Price::from("1.00000"));
        assert_eq!(f.commission, Some(Money::from("0.25 USD")));
        e.advance_time_impl(9.into(), &clocks).unwrap();
        let before = [
            snapshot(&e, &early).event_count(),
            snapshot(&e, &late).event_count(),
            snapshot(&e, &legacy).event_count(),
        ];
        e.advance_time_impl(9.into(), &clocks).unwrap();
        e.advance_time_impl(10.into(), &clocks).unwrap();
        let after = [
            snapshot(&e, &early).event_count(),
            snapshot(&e, &late).event_count(),
            snapshot(&e, &legacy).event_count(),
        ];
        assert_eq!(before, after);
        assert_eq!(terminal(&snapshot(&e, &early)), vec![("Expired".into(), 5)]);
        assert_eq!(terminal(&snapshot(&e, &late)), vec![("Expired".into(), 9)]);
        economics(&snapshot(&e, &early), 0);
        economics(&snapshot(&e, &late), 0);
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"mixed","causal_early":"Expired@5","causal_late":"Expired@9","legacy":"Filled8@1.00000 feeUSD0.25","repeat_events_unchanged":true})
        );
    }
    #[rstest]
    fn contingent_expiry_repeats(
        #[values(ContingencyType::Oco, ContingencyType::Ouo, ContingencyType::Oto)]
        kind: ContingencyType,
    ) {
        let mut e = engine();
        venue(&mut e, "CA", true, true);
        let id = instrument(&mut e, "EUR/USD.CA");
        let first_id = ClientOrderId::from("PARENT");
        let second_id = ClientOrderId::from("PEER");
        let first = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(first_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("1.00000"))
            .time_in_force(TimeInForce::Gtd)
            .expire_time(5.into())
            .contingency_type(kind)
            .linked_order_ids(vec![second_id])
            .build();
        let mut b = OrderTestBuilder::new(OrderType::Limit);
        b.trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(second_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("0.95000"));
        if kind == ContingencyType::Oto {
            b.parent_order_id(first_id);
        } else {
            b.contingency_type(kind).linked_order_ids(vec![first_id]);
        }
        let second = b.build();
        cache(&e, &first);
        cache(&e, &second);
        submit(&first, 2);
        submit(&second, 2);
        e.settle_venues(2.into(), SettlementScope::All);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(5.into(), &clocks).unwrap();
        e.drain_command_queues();
        let a = snapshot(&e, &first);
        let b = snapshot(&e, &second);
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"contingent","kind":format!("{:?}",kind),"first":format!("{:?}",a.status()),"second":format!("{:?}",b.status()),"first_terminals":terminal(&a),"second_terminals":terminal(&b)})
        );
        assert_eq!(a.status(), OrderStatus::Expired);
        assert_eq!(b.status(), OrderStatus::Canceled);
        assert_eq!(terminal(&a), vec![("Expired".into(), 5)]);
        assert_eq!(terminal(&b), vec![("Canceled".into(), 5)]);
        economics(&a, 0);
        economics(&b, 0);
        e.advance_time_impl(6.into(), &clocks).unwrap();
        e.drain_command_queues();
        assert_eq!(snapshot(&e, &first).event_count(), a.event_count());
        assert_eq!(snapshot(&e, &second).event_count(), b.event_count());
    }
    #[test]
    fn repeated_equal_time_timer_callbacks_observe_one_terminal() {
        let (mut e, o) = setup(true, true, Some(5));
        let observed = Rc::new(RefCell::new(Vec::new()));
        callback(&mut e, &o, 5, "read", "FIRST-TIMER", observed.clone());
        callback(&mut e, &o, 5, "read", "SECOND-TIMER", observed.clone());
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(5.into(), &clocks).unwrap();
        e.finalize_timestamp(&clocks, 5.into(), SettlementScope::All)
            .unwrap();
        e.finalize_timestamp(&clocks, 5.into(), SettlementScope::All)
            .unwrap();
        assert_eq!(
            *observed.borrow(),
            vec![(OrderStatus::Expired, 5), (OrderStatus::Expired, 5)]
        );
        let result = snapshot(&e, &o);
        assert_eq!(terminal(&result), vec![("Expired".into(), 5)]);
        economics(&result, 0);
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"duplicate-timers","callbacks":2,"expired_events":1,"fills":0,"fees":0})
        );
    }
    #[test]
    fn serialized_policy_defaults_stay_disabled() {
        let c: nautilus_execution::matching_engine::config::OrderMatchingEngineConfig =
            serde_json::from_str("{}").unwrap();
        assert!(!c.gtd_expiry_before_match);
        assert!(c.support_gtd_orders);
        let v = SimulatedVenueConfig::builder()
            .venue(Venue::from("DEFAULT"))
            .oms_type(OmsType::Netting)
            .account_type(AccountType::Cash)
            .book_type(BookType::L1_MBP)
            .starting_balances(vec![Money::from("1000.00 USD")])
            .build()
            .unwrap();
        assert!(!v.gtd_expiry_before_match);
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"defaults","policy":false})
        );
    }

    #[rstest]
    fn due_timer_cannot_create_execution(#[values(false, true)] partial: bool) {
        let (mut e, o) = setup(true, true, Some(5));
        if partial {
            let clocks = e.collect_all_clocks();
            e.advance_time_impl(3.into(), &clocks).unwrap();
            quote(o.instrument_id(), 3, "1.00000", 2);
            e.advance_time_impl(4.into(), &clocks).unwrap();
            quote(o.instrument_id(), 4, "1.10000", 6);
        }
        let observed = Rc::new(RefCell::new(Vec::new()));
        callback(&mut e, &o, 5, "modify", "ECONOMIC-COUNTEREXAMPLE", observed);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(7.into(), &clocks).unwrap();
        let result = snapshot(&e, &o);
        let actual: Vec<_> = result
            .events()
            .iter()
            .filter_map(|x| {
                if let OrderEventAny::Filled(f) = x {
                    Some((
                        f.client_order_id.to_string(),
                        f.last_qty.to_string(),
                        f.last_px.to_string(),
                        f.commission.map(|x| x.to_string()),
                        f.ts_event.as_u64(),
                    ))
                } else {
                    None
                }
            })
            .collect();
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"timer-economics","partial":partial,"actual_fills":actual,"expected_fills":if partial {"ORIGINAL qty2 price1.00000 fee0.25USD at3; nothing at/after5"}else{"none"}})
        );
        assert_eq!(
            result.filled_qty(),
            Quantity::from(if partial { 2 } else { 0 }),
            "expiry callback must create no additional execution"
        );
        assert_eq!(actual.len(), if partial { 1 } else { 0 });
        if partial {
            assert_eq!(
                actual[0],
                (
                    "ORIGINAL".into(),
                    "2".into(),
                    "1.00000".into(),
                    Some("0.25 USD".into()),
                    3
                )
            );
        }
        assert_eq!(terminal(&result), vec![("Expired".into(), 5)]);
    }

    #[test]
    fn independent_native_graph_clocks() {
        let mut results = Vec::new();
        for at in [9_u64, 4] {
            results.push(
                std::thread::spawn(move || {
                    let (mut e, o) = setup(true, true, Some(5));
                    let clocks = e.collect_all_clocks();
                    e.advance_time_impl(at.into(), &clocks).unwrap();
                    let result = snapshot(&e, &o);
                    economics(&result, 0);
                    (
                        e.kernel.clock.borrow().timestamp_ns().as_u64(),
                        result.status(),
                        terminal(&result),
                    )
                })
                .join()
                .unwrap(),
            );
        }
        assert_eq!(
            results,
            vec![
                (9, OrderStatus::Expired, vec![("Expired".into(), 9)]),
                (4, OrderStatus::Accepted, vec![])
            ]
        );
        println!(
            "SCHED_RESULT {}",
            serde_json::json!({"case":"independent-clocks","first_clock":9,"second_clock":4,"first":"Expired@9","second":"Accepted"})
        );
    }
    // The order is still outside the matching core when its deadline passes.
    #[rstest]
    fn delayed_insert_cannot_fill_after_deadline(
        #[values(false, true)] causal: bool,
        #[values(3_u64, 4)] delay: u64,
    ) {
        use nautilus_execution::models::latency::{LatencyModelHandle, StaticLatencyModel};
        let mut e = engine();
        venue(&mut e, "CA", causal, true);
        let id = instrument(&mut e, "EUR/USD.CA");
        e.venues
            .get(&id.venue)
            .unwrap()
            .borrow_mut()
            .set_latency_model(LatencyModelHandle::new(StaticLatencyModel::new(
                Default::default(),
                nautilus_core::DurationNanos::new(delay),
                Default::default(),
                Default::default(),
            )));
        let o = order(&e, id, "DELAYED", Some(5));
        cache(&e, &o);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(2.into(), &clocks).unwrap();
        quote(id, 2, "1.00000", 10);
        submit(&o, 2);
        e.settle_venues(2.into(), SettlementScope::All);
        assert_eq!(snapshot(&e, &o).status(), OrderStatus::Submitted);
        let at = 2 + delay;
        e.advance_time_impl(at.into(), &clocks).unwrap();
        e.settle_venues(at.into(), SettlementScope::All);
        let result = snapshot(&e, &o);
        assert_eq!(
            result.status(),
            if causal {
                OrderStatus::Rejected
            } else {
                OrderStatus::Filled
            }
        );
        assert_eq!(
            result.filled_qty(),
            Quantity::from(if causal { 0 } else { 8 })
        );
        let fills: Vec<_> = result
            .events()
            .into_iter()
            .filter_map(|x| match x {
                OrderEventAny::Filled(f) => Some(f),
                _ => None,
            })
            .collect();
        assert_eq!(fills.len(), if causal { 0 } else { 1 });
        if !causal {
            assert_eq!(fills[0].last_px, Price::from("1.00000"));
            assert_eq!(fills[0].commission, Some(Money::from("0.25 USD")));
        }
    }
    #[rstest]
    fn overdue_oto_child_cannot_fill_on_activation(#[values(false, true)] causal: bool) {
        let mut e = engine();
        venue(&mut e, "CA", causal, true);
        let id = instrument(&mut e, "EUR/USD.CA");
        let parent = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(ClientOrderId::from("PARENT"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("1.00000"))
            .contingency_type(ContingencyType::Oto)
            .linked_order_ids(vec![ClientOrderId::from("CHILD")])
            .build();
        let child = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(ClientOrderId::from("CHILD"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("1.00000"))
            .time_in_force(TimeInForce::Gtd)
            .expire_time(5.into())
            .parent_order_id(parent.client_order_id())
            .build();
        cache(&e, &parent);
        cache(&e, &child);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(2.into(), &clocks).unwrap();
        submit(&parent, 2);
        submit(&child, 2);
        e.settle_venues(2.into(), SettlementScope::All);
        e.advance_time_impl(6.into(), &clocks).unwrap();
        quote(id, 6, "1.00000", 16);
        let result = snapshot(&e, &child);
        assert_eq!(snapshot(&e, &parent).filled_qty(), Quantity::from(8));
        assert_eq!(
            result.status(),
            if causal {
                OrderStatus::Rejected
            } else {
                OrderStatus::Filled
            }
        );
        assert_eq!(
            result.filled_qty(),
            Quantity::from(if causal { 0 } else { 8 })
        );
        let fills: Vec<_> = result
            .events()
            .into_iter()
            .filter_map(|x| match x {
                OrderEventAny::Filled(f) => Some(f),
                _ => None,
            })
            .collect();
        assert_eq!(fills.len(), if causal { 0 } else { 1 });
        if !causal {
            assert_eq!(fills[0].last_px, Price::from("1.00000"));
            assert_eq!(fills[0].commission, Some(Money::from("0.25 USD")));
        }
    }
    #[test]
    fn stale_oto_list_leg_emits_one_rejection() {
        let mut e = engine();
        venue(&mut e, "CA", true, true);
        let id = instrument(&mut e, "EUR/USD.CA");
        let rejected = Rc::new(Cell::new(0));
        let count = Rc::clone(&rejected);
        let exec = Rc::downgrade(&e.kernel.exec_engine);
        // Observe raw native dispatch, then forward to the unchanged real engine.
        msgbus::register_order_event_endpoint(
            MessagingSwitchboard::exec_engine_process(),
            nautilus_common::msgbus::TypedIntoHandler::from(move |event: OrderEventAny| {
                if matches!(&event,OrderEventAny::Rejected(r) if r.client_order_id==ClientOrderId::from("CHILD"))
                {
                    count.set(count.get() + 1);
                }
                if let Some(exec) = exec.upgrade() {
                    exec.borrow_mut().process(&event);
                }
            }),
        );
        let parent = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(ClientOrderId::from("PARENT"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("1.00000"))
            .contingency_type(ContingencyType::Oto)
            .linked_order_ids(vec![ClientOrderId::from("CHILD")])
            .build();
        let child = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(e.trader_id())
            .instrument_id(id)
            .client_order_id(ClientOrderId::from("CHILD"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(8))
            .price(Price::from("1.00000"))
            .time_in_force(TimeInForce::Gtd)
            .expire_time(5.into())
            .parent_order_id(parent.client_order_id())
            .build();
        cache(&e, &parent);
        cache(&e, &child);
        let clocks = e.collect_all_clocks();
        e.advance_time_impl(6.into(), &clocks).unwrap();
        quote(id, 6, "1.00000", 16);
        let list = nautilus_model::orders::OrderList::new(
            nautilus_model::identifiers::OrderListId::from("OVERDUE-LIST"),
            id,
            parent.strategy_id(),
            vec![parent.client_order_id(), child.client_order_id()],
            6.into(),
        );
        send_execution_command(TradingCommand::SubmitOrderList(
            nautilus_common::messages::execution::SubmitOrderList::new(
                parent.trader_id(),
                Some(ClientId::from("CA")),
                parent.strategy_id(),
                list,
                vec![parent.init_event().clone(), child.init_event().clone()],
                None,
                None,
                None,
                UUID4::new(),
                6.into(),
                None,
            ),
        ));
        e.settle_venues(6.into(), SettlementScope::All);
        let result = snapshot(&e, &child);
        assert_eq!(snapshot(&e, &parent).filled_qty(), Quantity::from(8));
        assert_eq!(result.status(), OrderStatus::Rejected);
        assert_eq!(result.filled_qty(), Quantity::from(0));
        assert!(
            !result
                .events()
                .into_iter()
                .any(|e| matches!(e, OrderEventAny::Filled(_)))
        );
        assert_eq!(
            rejected.get(),
            1,
            "one native rejection, including raw dispatch"
        );
    }
}
