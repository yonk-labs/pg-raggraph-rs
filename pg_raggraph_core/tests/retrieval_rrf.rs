use pg_raggraph_core::retrieval::rrf::{LaneHit, RrfWeights, fuse};

#[test]
fn rrf_default_k_60_equal_weights() {
    let hits = vec![
        LaneHit {
            id: 1,
            lane: "vec",
            rk: 1,
        },
        LaneHit {
            id: 1,
            lane: "bm25",
            rk: 1,
        },
        LaneHit {
            id: 1,
            lane: "graph",
            rk: 1,
        },
    ];
    let scored = fuse(&hits, &RrfWeights::default());
    assert_eq!(scored.len(), 1);
    let expected = 3.0 / 61.0;
    let actual = scored[0].score;
    assert!(
        (actual - expected).abs() < 1e-12,
        "RRF k=60 equal weights, 3 hits at rank 1: expected {expected}, got {actual}"
    );
}

#[test]
fn rrf_two_chunks_one_vec_one_bm25() {
    let hits = vec![
        LaneHit {
            id: 1,
            lane: "vec",
            rk: 1,
        },
        LaneHit {
            id: 2,
            lane: "bm25",
            rk: 1,
        },
    ];
    let scored = fuse(&hits, &RrfWeights::default());
    assert_eq!(scored.len(), 2);
    let a = scored.iter().find(|s| s.id == 1).unwrap();
    let b = scored.iter().find(|s| s.id == 2).unwrap();
    assert!(
        (a.score - b.score).abs() < 1e-12,
        "ties under equal weights"
    );
    assert!((a.score - 1.0 / 61.0).abs() < 1e-12);
}

#[test]
fn rrf_weight_override_zeros_bm25_doubles_vec() {
    let hits = vec![
        LaneHit {
            id: 1,
            lane: "vec",
            rk: 1,
        },
        LaneHit {
            id: 1,
            lane: "bm25",
            rk: 1,
        },
        LaneHit {
            id: 1,
            lane: "graph",
            rk: 1,
        },
    ];
    let weights = RrfWeights {
        vec: 2.0,
        bm25: 0.0,
        graph: 1.0,
    };
    let scored = fuse(&hits, &weights);
    let expected = 2.0 / 61.0 + 0.0 / 61.0 + 1.0 / 61.0;
    assert!((scored[0].score - expected).abs() < 1e-12);
}

#[test]
fn rrf_descending_score_order() {
    let hits = vec![
        LaneHit {
            id: 1,
            lane: "vec",
            rk: 1,
        },
        LaneHit {
            id: 2,
            lane: "vec",
            rk: 1,
        },
        LaneHit {
            id: 2,
            lane: "bm25",
            rk: 1,
        },
    ];
    let scored = fuse(&hits, &RrfWeights::default());
    assert_eq!(scored.len(), 2);
    assert_eq!(scored[0].id, 2, "fuse() returns highest score first");
    assert_eq!(scored[1].id, 1);
    assert!(scored[0].score > scored[1].score);
}

#[test]
fn rrf_empty_input_yields_empty_output() {
    let scored = fuse(&[], &RrfWeights::default());
    assert!(scored.is_empty());
}
