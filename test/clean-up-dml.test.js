"use strict";

const chai = require("chai"),
  expect = chai.expect,
  { Readable } = require("stream"),
  { cleanUpDML } = require("../lib/helpers.js");

chai.should();

// Athena writes NULL cells as empty CSV fields: a single-column all-NULL row
// is an entirely empty line (`select null as foo` -> `foo\n\n`), a
// multi-column one keeps its separators (`,\n`). Every row must come back
// with every column present, `null` where the cell is blank.
describe("cleanUpDML", () => {
  function contextFor(columnInfo) {
    return {
      s3Metadata: Promise.resolve({
        ResultSet: { ResultSetMetadata: { ColumnInfo: columnInfo } },
      }),
    };
  }

  function inputFrom(csvText) {
    return Readable.from(Buffer.from(csvText));
  }

  const singleColumn = [{ Name: "foo", Type: "varchar" }];
  const threeColumns = [
    { Name: "foo", Type: "varchar" },
    { Name: "bar", Type: "integer" },
    { Name: "baz", Type: "boolean" },
  ];

  it("returns a single-column all-NULL row (empty CSV line)", async () => {
    const rows = await cleanUpDML(
      inputFrom("foo\n\n"),
      false,
      contextFor(singleColumn)
    );

    expect(rows).to.deep.equal([{ foo: null }]);
  });

  it("returns single-column NULL rows mixed with data rows", async () => {
    const rows = await cleanUpDML(
      inputFrom('foo\n"a"\n\n"b"\n'),
      false,
      contextFor(singleColumn)
    );

    expect(rows).to.deep.equal([{ foo: "a" }, { foo: null }, { foo: "b" }]);
  });

  it("returns a multi-column all-NULL row", async () => {
    const rows = await cleanUpDML(
      inputFrom("foo,bar,baz\n,,\n"),
      false,
      contextFor(threeColumns)
    );

    expect(rows).to.deep.equal([{ foo: null, bar: null, baz: null }]);
  });

  it("types cells and returns NULL cells as null-valued keys", async () => {
    const rows = await cleanUpDML(
      inputFrom('foo,bar,baz\n"hello",123,true\n"bye",789,\n'),
      false,
      contextFor(threeColumns)
    );

    expect(rows).to.deep.equal([
      { foo: "hello", bar: 123, baz: true },
      { foo: "bye", bar: 789, baz: null },
    ]);
  });

  it("keys rows by the metadata column names in column order", async () => {
    const rows = await cleanUpDML(
      inputFrom('foo,bar,baz\n"hello",123,true\n'),
      false,
      contextFor(threeColumns)
    );

    expect(Object.keys(rows[0])).to.deep.equal(["foo", "bar", "baz"]);
  });

  it("still skips entirely empty lines when ignoreEmpty is true", async () => {
    const rows = await cleanUpDML(
      inputFrom('foo\n"a"\n\n"b"\n'),
      true,
      contextFor(singleColumn)
    );

    expect(rows).to.deep.equal([{ foo: "a" }, { foo: "b" }]);
  });
});
