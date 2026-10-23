// bsv-low #558: `TopicManager::abort_admissible_outputs`, the reference's
// `abortAdmissibleOutputs`. Included in `engine::tests`.

/// A stateful manager: a real (not dry-run) identify makes provisional state
/// for the txid, which only an abort releases; every abort is recorded.
#[derive(Clone, Default)]
struct I558Stateful {
    provisional: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
    aborted: std::rc::Rc<std::cell::RefCell<Vec<Vec<u32>>>>,
    fail_abort: bool,
}

#[async_trait(?Send)]
impl TopicManagerTrait for I558Stateful {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if !context.dry_run {
            self.provisional.borrow_mut().push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn abort_admissible_outputs(
        &self,
        _beef: &[u8],
        outputs_to_abort: &[u32],
    ) -> Result<(), TopicManagerError> {
        self.aborted.borrow_mut().push(outputs_to_abort.to_vec());
        self.provisional.borrow_mut().clear();
        if self.fail_abort {
            return Err(TopicManagerError::Other("the release faulted".into()));
        }
        Ok(())
    }

    fn aborts_admissions(&self) -> bool {
        true
    }

    async fn get_documentation(&self) -> String {
        String::new()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

fn i558_engine(
    managers: Vec<(&str, Box<dyn TopicManagerTrait>)>,
    arc: Option<MockArcBroadcaster>,
) -> Engine {
    Engine::with_all(
        managers
            .into_iter()
            .map(|(topic, manager)| (topic.to_string(), manager))
            .collect(),
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        None,
        arc.map(|arc| Box::new(arc) as Box<dyn ArcBroadcaster>),
        None,
        EngineConfig::default(),
    )
}

/// The engine's abort is the reference's `abortProvisionalAdmissions`: each
/// admitting topic's manager told ONCE with its outputs (its provisional
/// state released), a topic with no outputs or no manager skipped, a pure
/// manager's default a no-op, and a manager whose abort errs logged while
/// the rest are still told. Does not compile on the base (65027163): the
/// method and the trait member do not exist there.
#[tokio::test]
async fn i558_a_each_admitting_topic_is_told_once_and_the_rest_are_skipped() {
    let stateful = I558Stateful::default();
    let failing = I558Stateful {
        fail_abort: true,
        ..I558Stateful::default()
    };
    let engine = i558_engine(
        vec![
            ("tm_failing", Box::new(failing.clone())),
            ("tm_stateful", Box::new(stateful.clone())),
            ("tm_pure", Box::new(MockTopicManager::admitting(vec![0]))),
        ],
        None,
    );
    let tagged = test_tagged_beef(vec!["tm_failing", "tm_stateful", "tm_pure"]);
    engine
        .submit(&tagged, SubmitMode::HistoricalTx)
        .await
        .unwrap();
    assert_eq!(
        stateful.provisional.borrow().len(),
        1,
        "the admission's state"
    );
    let told = engine
        .abort_provisional_admissions(
            &tagged.beef,
            &[
                ("tm_failing".into(), vec![0]),
                ("tm_stateful".into(), vec![0, 1]),
                ("tm_pure".into(), vec![0]),
                ("tm_stateful".into(), vec![]),
                ("tm_unknown".into(), vec![0]),
            ],
        )
        .await;
    assert_eq!(
        told, 3,
        "failing, stateful and pure; not the empty or unknown"
    );
    assert_eq!(
        *stateful.aborted.borrow(),
        vec![vec![0, 1]],
        "once, its outputs"
    );
    assert!(stateful.provisional.borrow().is_empty(), "released");
    assert_eq!(
        *failing.aborted.borrow(),
        vec![vec![0]],
        "told; its Err logged"
    );
}

/// Never on a clean admission, and never from `submit`: the engine's ARC
/// broadcast is non-fatal (the reference's default
/// `throwOnBroadcastFailure = false`, under which it does not abort
/// either, `Engine.ts:1219-1223`), so a `current-tx` submit whose broadcast
/// fails admits and aborts nothing, as one whose broadcast succeeds.
#[tokio::test]
async fn i558_b_a_submit_never_aborts_and_neither_does_a_failed_broadcast() {
    for arc in [MockArcBroadcaster::new(), MockArcBroadcaster::failing()] {
        let stateful = I558Stateful::default();
        let calls = arc.calls.clone();
        let engine = i558_engine(vec![("tm_stateful", Box::new(stateful.clone()))], Some(arc));
        let steak = engine
            .submit(
                &test_tagged_beef(vec!["tm_stateful"]),
                SubmitMode::CurrentTx,
            )
            .await
            .unwrap();
        assert_eq!(steak["tm_stateful"].outputs_to_admit, vec![0]);
        assert_eq!(calls.lock().unwrap().len(), 1, "broadcast once");
        assert_eq!(stateful.provisional.borrow().len(), 1);
        assert!(
            stateful.aborted.borrow().is_empty(),
            "never aborted by submit"
        );
    }
}
