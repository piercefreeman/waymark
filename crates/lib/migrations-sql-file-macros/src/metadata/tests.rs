//! Tests of reading the metadata header of an SQL file.

use super::{ParseError, parse};

#[test]
fn header_with_a_revision_alone_runs_inside_a_transaction() {
    let metadata = parse("-- revision: widgets-1\n\nCREATE TABLE widgets (id INT);\n")
        .expect("the header is read");

    assert_eq!(metadata.revision, "widgets-1");
    assert!(metadata.runs_inside_a_transaction);
}

#[test]
fn no_transaction_annotation_is_read_before_or_after_the_revision() {
    for sql in [
        "-- revision: widgets-label-1\n-- no-transaction\n\nCREATE INDEX CONCURRENTLY i ON t (c);\n",
        "-- no-transaction\n-- revision: widgets-label-1\n\nCREATE INDEX CONCURRENTLY i ON t (c);\n",
    ] {
        let metadata = parse(sql).expect("the header is read");

        assert_eq!(metadata.revision, "widgets-label-1");
        assert!(!metadata.runs_inside_a_transaction);
    }
}

#[test]
fn header_stops_at_the_first_empty_line() {
    let metadata = parse(
        "-- revision: widgets-1\n\n-- What the migration is for.\n-- no-transaction\nSELECT 1;\n",
    )
    .expect("the header is read");

    assert_eq!(metadata.revision, "widgets-1");
    assert!(metadata.runs_inside_a_transaction);
}

#[test]
fn header_of_a_file_without_an_empty_line_is_all_of_it() {
    let metadata = parse("-- revision: nothing-1").expect("the header is read");

    assert_eq!(metadata.revision, "nothing-1");
}

#[test]
fn revision_is_any_text_without_the_whitespace_around_it() {
    let metadata = parse("-- revision:   second take: the widgets  \r\n\r\nSELECT 1;\r\n")
        .expect("the header is read");

    assert_eq!(metadata.revision, "second take: the widgets");
}

#[test]
fn file_without_a_revision_is_refused() {
    for sql in [
        "",
        "\n-- revision: widgets-1\n",
        "-- no-transaction\n\nSELECT 1;\n",
    ] {
        assert!(matches!(parse(sql), Err(ParseError::NoRevision)));
    }
}

#[test]
fn revision_without_a_value_is_refused() {
    for sql in ["-- revision\n", "-- revision:\n", "-- revision:   \n"] {
        let Err(error @ ParseError::NoValue { .. }) = parse(sql) else {
            panic!("the revision has no value");
        };
        assert_eq!(
            error.to_string(),
            "line 1: the `revision` annotation has no value"
        );
    }
}

#[test]
fn no_transaction_with_a_value_is_refused() {
    let Err(error @ ParseError::UnexpectedValue { .. }) =
        parse("-- revision: widgets-1\n-- no-transaction: yes\n")
    else {
        panic!("the annotation takes no value");
    };

    assert_eq!(
        error.to_string(),
        "line 2: the `no-transaction` annotation takes no value"
    );
}

#[test]
fn annotation_written_twice_is_refused() {
    let Err(revision_again @ ParseError::RepeatedAnnotation { .. }) =
        parse("-- revision: widgets-1\n-- revision: widgets-2\n")
    else {
        panic!("the revision is there twice");
    };
    let Err(no_transaction_again @ ParseError::RepeatedAnnotation { .. }) =
        parse("-- no-transaction\n-- revision: widgets-1\n-- no-transaction\n")
    else {
        panic!("the annotation is there twice");
    };

    assert_eq!(
        revision_again.to_string(),
        "line 2: the metadata header has the `revision` annotation more than once"
    );
    assert_eq!(
        no_transaction_again.to_string(),
        "line 3: the metadata header has the `no-transaction` annotation more than once"
    );
}

#[test]
fn comment_in_the_header_that_is_not_an_annotation_is_refused() {
    let Err(error @ ParseError::UnknownAnnotation { .. }) =
        parse("-- revision: widgets-1\n-- The widgets.\n\nSELECT 1;\n")
    else {
        panic!("the comment is not an annotation the header can have");
    };

    assert_eq!(
        error.to_string(),
        "line 2: the metadata header has no `The widgets.` annotation: it is every line down \
         to the first empty one, each `-- revision: <revision>` or `-- no-transaction`"
    );
}

#[test]
fn line_in_the_header_that_is_not_a_comment_is_refused() {
    for sql in [
        "-- revision: widgets-1\nCREATE TABLE widgets (id INT);\n",
        "--revision: widgets-1\n",
        "#!/usr/bin/env psql\n-- revision: widgets-1\n",
    ] {
        assert!(matches!(
            parse(sql),
            Err(ParseError::NotAnAnnotation { .. })
        ));
    }

    let Err(error) = parse("-- revision: widgets-1\nCREATE TABLE widgets (id INT);\n") else {
        panic!("the statement is not an annotation");
    };
    assert_eq!(
        error.to_string(),
        "line 2 is in the metadata header and is not an annotation: the header is every line \
         down to the first empty one, each `-- revision: <revision>` or `-- no-transaction`"
    );
}
