use crate::expression::*;

use Transformation as Tx;

pub(crate) fn optimise(computations: &Vec<Computation>) -> Vec<Computation> {
    let mut output = vec![];
    let mut input = Computation::empty();
    for c in computations {
        // if computation is a join, recurse into it
        if let [Tx::Join(a, b, criteria)] = &c.transformations[..] {
            // optimise a and b
            let a = optimise(a);
            let b = optimise(b);
            let join = Computation {
                input: c.input.clone(),
                transformations: vec![Tx::Join(a, b, criteria.clone())],
                output: c.output.clone(),
            };
            // push the join into output, given that join will always be the last like a read, we might have to perform its optimisations here
            // TODO: join could create an infinite loop here
            // TODO: Can join be optimised with other computations?
            if !input.transformations.is_empty() {
                output.push(input.clone());
            }
            output.push(join.clone());
            continue;
        }
        if input.transformations.is_empty() {
            input = c.clone();
            continue;
        }
        // compare current input with computation
        // chain non-joining computations (input can never be a join)
        // dbg!((c.input.len(), input.input.len()));
        match (c.input.len(), input.input.len()) {
            // a read and some other transformation
            (0, 1) => {
                if let [Tx::Read(reader)] = &c.transformations[..] {
                    match &input.transformations[..] {
                        [x] => {
                            let (mut out, input_) = optimise_read(&input, c, x, reader);
                            input = input_;
                            output.append(&mut out);
                        }
                        _ => {
                            output.push(input.clone());
                            input = c.clone();
                            continue;
                        }
                    }
                } else {
                    output.push(input.clone());
                    input = c.clone();
                    continue;
                }
            }
            (1, 1) => {
                // subsume limits or push them up
                match (&input.transformations[..], &c.transformations[..]) {
                    ([Tx::Limit(a)], [Tx::Limit(b)]) => {
                        input.transformations = vec![Tx::Limit(*a.min(b))];
                        output.push(input.clone());
                        continue;
                    }
                    (_, [Tx::Limit(_)]) => {
                        // we might not need to do anything, depending on the other transformation
                        output.push(input.clone());
                        input = c.clone();
                        continue;
                    }
                    ([Tx::Limit(_)], _) => {
                        // swap limit (input) with current transformation
                        output.push(c.clone());
                        // we retain the input as the limit
                        continue;
                    }
                    (x @ [Tx::Select(_) | Tx::Drop(_)], [Tx::Calculate(calc)]) => {
                        // evaluate if select affects columns in calculate, and swap/drop where necessary
                        // TODO: write unit test for this optimisation
                        // TODO: complete the Drop implementation
                        let (mut out, input_) = optimise_project_calc(&input, c, &x[0], calc);
                        input = input_;
                        output.append(&mut out);
                    }
                    (_, _) => {
                        output.push(input.clone());
                        input = c.clone();
                        continue;
                    }
                }
            }
            (_, _) => {
                output.push(input.clone());
                input = c.clone();
                continue;
            }
        }
    }

    output
}

fn optimise_read(
    input: &Computation,
    read: &Computation,
    x: &Transformation,
    reader: &Reader,
) -> (Vec<Computation>, Computation) {
    let mut output = vec![];
    let mut mutated = read.clone();

    match &reader.source {
        DataSourceType::Arrow(_) | DataSourceType::Json(_) | DataSourceType::Parquet(_) => {
            // no projection support
            output.push(input.clone());
        }
        DataSourceType::Csv(path, options) => {
            // projection supported
            let mut options = options.clone();
            match x {
                Transformation::Select(fields) => {
                    // if data source supports projection, merge the select with the read
                    // get the position of columns from dataset
                    if options.projection.is_none() {
                        // simple case, pass the projection through
                        let mut out_ds = vec![];
                        let proj = input
                            .output
                            .columns
                            .iter()
                            .enumerate()
                            .filter_map(|(i, c)| {
                                if fields.contains(&c.name) {
                                    out_ds.push(c.clone());
                                    Some(i)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<usize>>();
                        options.projection = Some(proj);
                        let reader = Reader {
                            source: DataSourceType::Csv(path.clone(), options),
                        };
                        mutated = Computation {
                            input: read.input.clone(),
                            transformations: vec![Transformation::Read(reader)],
                            output: Dataset {
                                name: read.output.name.clone(),
                                columns: out_ds,
                            },
                        };
                        // TODO: this should be the last computation
                        output.push(mutated.clone());
                    } else {
                        // TODO: join select projections together
                        output.push(input.clone());
                    }
                }
                Transformation::Drop(fields) => {
                    output.push(input.clone());
                }
                Transformation::Limit(limit) => {
                    match options.max_records {
                        Some(max) => options.max_records = Some(max.min(*limit)),
                        None => options.max_records = Some(*limit),
                    }
                    let reader = Reader {
                        source: DataSourceType::Csv(path.clone(), options),
                    };
                    mutated = Computation {
                        input: read.input.clone(),
                        transformations: vec![Transformation::Read(reader)],
                        output: read.output.clone(),
                    };
                    output.push(mutated.clone());
                }
                _ => {
                    output.push(input.clone());
                }
            }
        }
        DataSourceType::Sql(_, _) => {
            // projection supported,
            output.push(input.clone());
        }
    }

    (output, mutated)
}

fn optimise_project_calc(
    input: &Computation,
    project: &Computation,
    x: &Transformation,
    calc: &Calculation,
) -> (Vec<Computation>, Computation) {
    let mut output = vec![];
    let mut mutated = project.clone();

    match x {
        Tx::Select(cols) => {
            // if calculated column is not in select, drop calculation
            let selected_output = cols.contains(&calc.output.name);
            if !selected_output {
                // drop calculation
                return (output, mutated);
            }
            // check if all the inputs are included in the select
            let calc_inputs: Vec<&str> = calc.inputs.iter().map(|col| col.name()).collect();
            let selected_inputs = cols
                .iter()
                .filter(|proj| calc_inputs.contains(&proj.as_str()))
                .count();
            if selected_inputs == calc_inputs.len() {
                // all calculation fields are selected
                // rewrite select without the calculated field, and push it up
                output.push(input.clone());
                mutated.output = Dataset {
                    name: mutated.output.name.clone(),
                    columns: mutated
                        .output
                        .columns
                        .into_iter()
                        .filter(|proj| proj.name != calc.output.name)
                        .collect(),
                };
                return (output, mutated);
            }
        }
        Tx::Drop(cols) => {
            output.push(input.clone());
        }
        _ => unreachable!(),
    }
    (output, mutated)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::lazyframe::LazyFrame;

    #[test]
    fn test_read_project() {
        let reader = Reader {
            source: DataSourceType::Csv(
                "test/data/uk_cities_with_headers.csv".to_string(),
                CsvReadOptions {
                    has_headers: true,
                    delimiter: Some(b','),
                    max_records: None,
                    batch_size: 1024,
                    projection: None,
                },
            ),
        };
        let computation = Computation::compute_read(&reader);
        let comp_clone = computation.clone();
        let computations = vec![
            Computation {
                input: vec![comp_clone.output.clone()],
                transformations: vec![Transformation::Select(vec![
                    "city".to_string(),
                    "lat".to_string(),
                    // "lng".to_string(),
                ])],
                output: comp_clone.output.clone(),
            },
            // read comes last
            computation,
        ];
        let optimised = optimise(&computations);
        assert_eq!(optimised.len(), 1);
        assert_eq!(optimised[0].output.columns.len(), 2);
    }

    #[test]
    fn test_read_limit_project() {
        let reader = Reader {
            source: DataSourceType::Csv(
                "test/data/uk_cities_with_headers.csv".to_string(),
                CsvReadOptions {
                    has_headers: true,
                    delimiter: Some(b','),
                    max_records: None,
                    batch_size: 1024,
                    projection: None,
                },
            ),
        };
        let computation = Computation::compute_read(&reader);
        let mut frame = LazyFrame::read(computation);
        frame = frame.select(vec!["city", "lat"]).unwrap();
        frame = frame.limit(32);
        let computations = frame.expression.unroll();
        assert_eq!(computations.len(), 3);
        let optimised = optimise(&computations);
        // println!("Optimised: {:#?}", optimised);
        assert_eq!(optimised.len(), 2);
        assert_eq!(optimised[0].output.columns.len(), 2);
        // optimise again to join the select with the limit
        let optimised = optimise(&optimised);
        assert_eq!(optimised.len(), 1);
        assert_eq!(optimised[0].output.columns.len(), 2);
    }

    #[test]
    fn test_join() {
        let reader = Reader {
            source: DataSourceType::Csv(
                "test/data/uk_cities_with_headers.csv".to_string(),
                CsvReadOptions {
                    has_headers: true,
                    delimiter: Some(b','),
                    max_records: None,
                    batch_size: 1024,
                    projection: None,
                },
            ),
        };
        let computation = Computation::compute_read(&reader);
        let mut frame = LazyFrame::read(computation);
        frame = frame.select(vec!["city", "lat", "lng"]).unwrap();
        frame = frame.limit(32);
        frame = frame.with_column_renamed("city", "town");
        frame = frame
            .with_column(
                "sin_lat",
                Function::Scalar(ScalarFunction::Sine),
                vec!["lat"],
                None,
            )
            .unwrap();
        frame = frame
            .with_column(
                "sin_lng",
                Function::Scalar(ScalarFunction::Sine),
                vec!["lng"],
                None,
            )
            .unwrap();
        let reader = Reader {
            source: DataSourceType::Csv(
                "./test/data/uk_cities_with_headers.csv".to_owned(),
                CsvReadOptions {
                    has_headers: true,
                    batch_size: 1024,
                    delimiter: None,
                    max_records: Some(1024),
                    projection: None,
                },
            ),
        };
        let compute = Computation::compute_read(&reader);
        let frame2 = LazyFrame::read(compute);
        frame = frame
            .join(
                &frame2,
                &JoinCriteria {
                    criteria: vec![("town".to_owned(), "city".to_owned())],
                    join_type: JoinType::InnerJoin,
                },
            )
            .unwrap();
        let computations = frame.expression.unroll();
        assert_eq!(computations.len(), 1);
        let optimised = optimise(&computations);
    }
    #[test]
    fn test_filter() {
        let reader = Reader {
            source: DataSourceType::Csv(
                "test/data/uk_cities_with_headers.csv".to_string(),
                CsvReadOptions {
                    has_headers: true,
                    delimiter: Some(b','),
                    max_records: None,
                    batch_size: 1024,
                    projection: None,
                },
            ),
        };
        let computation = Computation::compute_read(&reader);
        let mut frame = LazyFrame::read(computation);
        frame = frame.select(vec!["city", "lat", "lng"]).unwrap();
        let filter = BooleanFilter::Gt(
            Box::new(BooleanFilter::Input(BooleanInput::Column(
                frame.column("lat").unwrap().1.clone(),
            ))),
            Box::new(BooleanFilter::Input(BooleanInput::Scalar(Scalar::Int64(0)))),
        );
        frame = frame.filter(filter);
        let computations = frame.expression.unroll();
        assert_eq!(computations.len(), 3);
        let optimised = optimise(&computations);
        dbg!(optimised);
    }
}
