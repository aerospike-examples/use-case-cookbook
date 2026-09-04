# Aerospike Expression Language (AEL) — Canonical Reference

(Saved from Tim Faulkes' Confluence doc, shared 2026-08-28. This is ground truth for AEL syntax
in this repo — prefer this over any third-party grammar repo when writing or debugging AEL.)

AEL is a text expression language evaluated server-side on Aerospike records. Expressions read and
transform bin values, record metadata, and collection data for filters, read expressions, and
write expressions.

**Table conventions.** `—` in a cell means *not applicable* in that column's context: in selector
tables (§5, §6), the form is unavailable for that dimension; in function parameter tables, there
are no parameters. §3.4 uses descriptive text in the *Existing value* column rather than `—`.

## 1. Lexical rules

### 1.1 Whitespace
Spaces, tabs, and newlines are allowed between tokens and are ignored.

### 1.2 Comments
Block comments only: `/* … */`. Comments may appear wherever whitespace is allowed. Nested block
comments are not allowed.

### 1.3 Identifiers
Unquoted identifiers match `[A-Za-z_][A-Za-z0-9_]*`. Used for bin path segments (when the name is a
legal bare identifier), function names, and `let` variable names. Variable names cannot be quoted.

### 1.4 Reserved words (lowercase)
| Word | Role |
|---|---|
| and, or | Logical operators |
| not | Logical negation (function form) |
| in | Membership test |
| let, then | Local bindings |
| when, default | Conditional expression |
| unknown, error | Trilean value literals (e.g. `default => unknown` in `when`; see §9.1) |
| true, false | Boolean literals |

### 1.5 Language constants (UPPERCASE)
| Token | Role |
|---|---|
| NIL | Lowest value in CDT ordering for comparisons |
| INF | Highest value in CDT ordering for comparisons |
| * | Wildcard value inside list or map literals only |
| INT, FLOAT, STRING, BOOL, BLOB, LIST, MAP, GEO, HLL | Particle-type value constants (e.g. `$.data.type() == INT`) |
| VECTOR | Not used yet, reserved for future capabilities |

Type-name constants are valid in value position for comparisons against `type()` results. They are
also used as `:TYPE` suffixes on paths (`$.bin:INT`, `@:FLOAT`, …).

### 1.6 Function arguments
Function and method arguments generally use `name: value` syntax such as `log(value: 128, base: 2)`.

For every function that uses named parameters, every argument must be labelled; labels may appear
in any order (e.g. `log(value: 128, base: 2)` and `log(base: 2, value: 128)` are equivalent).

Exceptions are:
- Single-argument calls (`abs`, `ceil`, `floor`, `countOneBits`, `geoJson`, …): one positional
  argument, eg `abs(-3)`
- Variable number of arguments of the same type (eg `min` / `max`): positional eg `min(3, 5, 7, 2)`
- `geoCompare(a, b)`: positional; both arguments are GEO.

## 2. Literals

### 2.1 Integer
Decimal digits with optional leading `+` or `-`. Hexadecimal (`0x…`) and binary (`0b…`) forms are
supported.

### 2.2 Float
Single decimal point required; the point must not be the last character. A leading dot is allowed
(e.g. `.5`). `10.` is invalid — use `10.0`.

### 2.3 String
Single- or double-quoted. Single-line only in source: no raw newline between opening and closing
delimiter; use escape sequences for line breaks inside the value.

| Escape | Meaning |
|---|---|
| \\ | Backslash |
| \n | Newline |
| \t | Tab |
| \r | Carriage return |
| \" | Double quote (in double-quoted strings) |
| \' | Single quote (in single-quoted strings) |
| \0 | NUL byte |
| \xHH | Byte with hex value HH |

Quoted strings apply everywhere quotes are allowed: expression literals, map keys, quoted bin name
segments on paths. Unquoted path segments do not use escape processing.

### 2.4 Boolean
`true`, `false`

### 2.5 BLOB
Even-length hexadecimal with `x` or `X` prefix: `x'cafe'`, `X'ffee'`

### 2.6 Base64
`b64'…'` or `B64'…'`. Invalid base64 is a parse error. Empty payload `b64''` is allowed.

### 2.7 List
`[elem, …]` or `[]`.

Default ordering: a list literal without a suffix is unsorted.

Optional suffixes: `:SORTED` or `:UNSORTED` immediately after the closing `]` (e.g.
`[1, 2, 3]:SORTED`, `[a, b]:UNSORTED`). These imply the LIST type, and detail the type of list to
create if it doesn't exist. Without a suffix, a bin-level list will be created UNSORTED, but nested
lists that do not exist will fail when being accessed.

### 2.8 Map
`{key: value, …}` or `{}`. Keys may be strings, integers, or BLOB literals.

Default ordering: a map literal without a suffix is key-ordered (same ordering as `:KEY_ORDERED` on
a path segment — §3.4).

Optional suffix: `:UNORDERED` immediately after the closing `}` for an unordered map literal (e.g.
`{a: 1, b: 2}:UNORDERED`). There is no `:KEY_ORDERED` literal suffix — key-ordered is the default.
Key–value ordered maps (`:KEY_VALUE_ORDERED`) are not available in literal syntax; use a path
create suffix (§3.4) when materializing that container on navigation.

Map-literal `:UNORDERED` sets the stored value's ordering only. Path-segment `:UNORDERED` (§3.4)
does the same when creating a missing map on navigation. Both are distinct from `:UNORDERED` on
`getMaps()` (§17), which controls return shape when reading.

### 2.9 Regex
`/pattern/` or `/pattern/flags`. ICU Perl-compatible regex syntax. Flags compose by concatenation
(e.g. `/pat/im`).

| Flag | Meaning |
|---|---|
| i | Case-insensitive (Unicode case folding) |
| m | ^ and $ match line boundaries |
| s | Dot matches newlines |
| x | Free-spacing: unescaped whitespace ignored; # starts comment to end of line in pattern |
| w | Unicode-aware word boundaries for \b |

## 3. Types

### 3.1 Concrete types
| Type | Description |
|---|---|
| INT | Integer |
| FLOAT | Floating-point |
| STRING | Unicode string (code points for string functions) |
| BOOL | Boolean literal values — true and false only |
| TRILEAN | Three-valued logic result — true, false, or unknown (see §3.2) |
| BLOB | Byte array |
| LIST | Ordered collection |
| MAP | Key–value collection |
| GEO | GeoJSON value |
| HLL | HyperLogLog bin |

### 3.2 TRILEAN (three-valued logic)
Many predicates and logical combinations return TRILEAN, not plain BOOL. A TRILEAN result is one of:

| Value | Meaning |
|---|---|
| true | Definitively yes |
| false | Definitively no |
| unknown | Indeterminate — typically because a referenced bin, key, or path operand is absent or cannot be evaluated |

`unknown` is not `false`. In filters, an `unknown` result usually causes the expression to fail for
that record (the record is not selected). Whether failure is silent or surfaced to the application
as an error depends on application / API flags (for example filter vs read mode and explain
options) — not on AEL syntax.

`and` / `or` truth tables (a, b are TRILEAN):

| a and b | true | false | unknown |
|---|---|---|---|
| true | true | false | unknown |
| false | false | false | false |
| unknown | unknown | false | unknown |

| a or b | true | false | unknown |
|---|---|---|---|
| true | true | true | true |
| false | true | false | unknown |
| unknown | true | unknown | unknown |

`not(a)`: true → false; false → true; unknown → unknown.

Reserved literals `unknown` and `error` (§1.4, §9.1) are separate value forms used in expressions
such as `when (…, default => unknown)` — not the same as a predicate returning unknown because a
bin is missing.

### 3.3 Type suffixes on paths
Attach `:TYPE` to pin static type:

| Form | Meaning |
|---|---|
| `$.bin:INT` | Strict typing; participates in canonical type propagation |
| `$.bin:LOCAL:INT` | Loose typing for this occurrence only |
| `$.l.[0]:INT` | Type the value read at that selector |
| `$.m.key:STRING` | Type the value at map key |
| `@:INT` | Type loop variable @ in a filter or modify body |
| `@.price:FLOAT` | Type a field read from @ |
| `$.key():INT` | Optional return type on no-arg record helper |

Valid type names: INT, FLOAT, STRING, BOOL, BLOB, LIST, MAP, GEO, HLL. These are type pins on any
path operand — bin root (`$.bin:INT`), navigation tail (`$.m.k:STRING`), loop variable (`@:FLOAT`),
and so on.

`:MAP` and `:LIST` are type pins only — they tell the compiler to treat a path value as a map or
list (required on some reads, e.g. wildcard-first paths in §4.3). They do not create containers and
are not create-order flags. Missing bins are materialised by write verbs (e.g. `putItems`, `setTo`)
or by create-order suffixes on path segments (§3.4).

`toInt()` / `toFloat()` and type pins: the same method names are used for string parsing (§14) and
numeric casting (§12). A call such as `$.amount.toFloat()` does not tell the compiler whether
`$.amount` is numeric text to parse or an integer to cast — pin the source type on the path before
the call, e.g. `$.amount:INT.toFloat()` to cast an integer, or `$.amount:STRING.toFloat()` to parse
a string. Literals and other already-typed receivers need no suffix (`"1234".toInt()`).

Casing: path suffix modifiers and postfix flags use UPPERCASE (`:LOCAL:`, `:MAP`, `:KEY_ORDERED`,
`:SORTED`, `:NO_FAIL`, …).

Use of LOCAL: In some rare cases, a bin such as `$.amount` may hold different types such as INT or
FLOAT. The AEL normally determines the type of a bin and keeps this for the whole execution. For
example `$.b > 3 and $.a == $.b`, `$.b` is inferred to be INT by the first comparison, and carries
this forward to the second comparison.

`:LOCAL` types each occurrence for that branch only — it does not pin a single canonical type on
the bin record-wide. In a `when`, every branch must produce the same result type; here integer
amounts are cast to FLOAT so the branches unify and can be divided by `$.quantity` for an average
price:

```
when (
  $.amount.type() == INT => $.amount:LOCAL:INT.toFloat(),
  default => $.amount:LOCAL:FLOAT
) / $.quantity:INT.toFloat()
```

### 3.4 Collection path suffixes (create-order)
These suffixes attach to collection path segments, including the first segment after `$.` when a
top-level container may need to be created. Type pins `:MAP` / `:LIST` still live in §3.3 and
remain typing-only. Each flag tells the server how to create a missing container when navigation
or a write needs it. Create-order flags attach to the segment where the missing container should
be created — each nested level carries its own flag independently (e.g.
`$.a.b:SORTED.[0]:KEY_ORDERED.c.setTo(5)` creates a sorted list at `b`, then a key-ordered map at
index 0).

At most one create-order flag from the table below may follow a single path segment. Combining two
is a parse error. `:PERSIST_INDEX` (§17) may stack with a map create-order flag on the bin root
only. Other flags may also stack with the following, for example `:LOCAL`.

| Suffix | Missing value | Existing value |
|---|---|---|
| `:KEY_ORDERED` | Create empty key-ordered map | No-op for creation |
| `:KEY_VALUE_ORDERED` | Create empty key–value ordered map | No-op for creation |
| `:UNORDERED` | Create empty unordered map | No-op for creation |
| `:SORTED` | Create empty sorted list | No-op for creation |
| `:UNSORTED` | Create empty unsorted list (bounded — see below; default when list create is needed) | No-op for creation |
| `:UNSORTED_PAD` | Create empty unsorted list with nil-padding when navigation or a write needs a distant index (see below) | No-op for creation |

Vocabulary: map segments use `:KEY_ORDERED`, `:KEY_VALUE_ORDERED`, and `:UNORDERED`; list segments
use `:SORTED`, `:UNSORTED`, and `:UNSORTED_PAD`. At most one of `:SORTED`, `:UNSORTED`, and
`:UNSORTED_PAD` may follow a list segment.

**Bounded list writes (default).** Bounded is the default at two levels, controlled together unless
`:UNSORTED_PAD` opts out:
- Context create — when a missing list container is created on the navigation path, `:UNSORTED`
  (the default list create-order) does not nil-pad skipped slots.
- List write ops — when writing past the end of an existing, unsorted list (`setTo`, `insert`,
  `add`, `insertItems`), the server does not nil-pad to reach a sparse index. Writes at
  index == list size are contiguous append (including bulk `insertItems` at the end — all M items
  append in one call). Bounded forbids sparse growth (index strictly greater than size), not
  growth in general.

There is no `:BOUNDED` suffix — bounded needs no name. `:UNSORTED_PAD` is the single opt-out: it
enables nil-padding on both context create and list-write padding.

`:UNSORTED_PAD` — use with caution. This is not the default (that is bounded `:UNSORTED`, matching
the Java/C client default `pad=false` on container create). `:UNSORTED_PAD` opts in to sparse list
behaviour: if the path next navigates to an index beyond the list end, or a write targets such an
index, the server inserts NIL for every skipped slot. Writing at index 1,000,000 on an empty list
can materialize on the order of a million nil elements and greatly increase record size. Use only
when sparse lists are intentional; prefer `:UNSORTED`, append, or a nearby index when possible.

Note that SORTED lists can never be sparse so do not require this flag.

For example: `$.items:SORTED.[0]:KEY_ORDERED.field.setTo('x')` (nested create);
`$.sparse:UNSORTED_PAD.[1000000].setTo('value')` (sparse create — see warning above). A bare-bin
write such as `$.m:MAP.putItems({k: v})` creates a missing map bin via `putItems`, not via the
`:MAP` type pin (§3.3).

**Create-order suffix restrictions (by terminal kind).** Create-order flags (`:KEY_ORDERED`,
`:KEY_VALUE_ORDERED`, `:UNORDERED`, `:SORTED`, `:UNSORTED`, `:UNSORTED_PAD`) may appear only on
write paths that can materialise missing containers from CDT context-create bits. They are not
allowed on read terminals, and are not allowed on write terminals that do not create containers
(`modify()`, `remove()`). A create-order suffix on such paths is a parse error, not a silent no-op.
The rules:

1. **Suffix on a single-select segment.** The suffix must attach to a single-select segment — not
   to a wildcard, filter, or multi-key/rank/index/value list. Invalid:
   `$.m.*:KEY_ORDERED.k.setTo(1)`, `$.m.{@a,b}:KEY_ORDERED.k.setTo(1)`.
2. **Every segment before the leaf must be single-select.** An earlier wildcard or multi-select
   selector invalidates the whole path even when the suffix sits on a later segment. Invalid:
   `$.m.*.k:KEY_ORDERED.setTo(1)`.
3. **Terminals that do not create containers reject create-order.** `modify()` and `remove()` never
   accept create-order flags, even on fully single-select paths. Invalid:
   `$.m.p1:KEY_ORDERED.k.modify(@ + 1)`, `$.m.p1:KEY_ORDERED.k.remove()`.

When an absent CDT context segment (a path step missing in the bin) should be tolerated for write
terminals that do not create containers, use `:NO_FAIL` instead (for example,
`$.m.p1.{@a,b}.remove():NO_FAIL`).

Qualifying create-order paths honour context-create bits at compile time. Read terminals and
non-creating write terminals reject create-order because there is no container-creation step to
apply those bits.

## 4. Paths and navigation

### 4.1 Record and bin prefix
| Form | Meaning |
|---|---|
| `$` | Current record |
| `$.binName` | Bin named binName |
| `$."quoted\nname"` | Bin whose name requires quoting/escapes |
| `$.a.b.c` | Navigate map keys b, c under bin a |
| `$.a.[0]` | Navigate list index 0 under bin a |

Navigation is strict by default: missing intermediate keys or out-of-range indices cause failure
unless a create-if-missing suffix (`:KEY_ORDERED`, `:SORTED`, etc.) applies on that segment.

### 4.2 Parenthesised expressions
| Form | Meaning |
|---|---|
| `(expr)` | Grouping in ordinary expressions |
| `(expr).segment…` / `(expr).method(…)` | Use a parenthesised expression as the left side of further `.…` navigation or method calls (§14.5) |
| `terminal(expr)` | Full expressions in function-call arguments (e.g. `setTo($.otherBin)`, `bitSet(offset: 0, size: 8, value: $.data)`) |

When `.method()` must follow `(…)`. A dot chain can continue after `()` only when the left side is
already a bin path (`$.bin…`), a blob literal, a standalone function call (`max(…)`, `abs(…)`), or
an earlier method chain. Record metadata (§10) and other general expressions must be wrapped:
`(expr).method(…)` — e.g. `($.ttl()).toString()`, not `$.ttl().toString()` (parse error).

Parenthesised expressions are not currently supported inside selector brackets `{…}` or `[…]` in
place of literals (e.g. `$.m.{($.idx)}` is not valid).

Collection literals (`[…]`, `{…}` in value position — §2.7, §2.8) are static only: elements must be
literals, not bin paths or other `$` expressions (e.g. `[ $.hllA, $.hllB ]` in an `hllUnionCount`
argument is a parse error; see §16.1). This will change in a later release.

### 4.3 Wildcard iteration
| Form | Meaning |
|---|---|
| `*` | Between dots: iterate all children at this map or list level |
| `.*` | After a path segment: all children of that segment |
| `.*[?(predicate)]` | Children matching boolean predicate |

Wildcard `*` as a path segment is distinct from `*` as a literal value inside `[…]` or `{…}`.

When `*` is the first segment after a bin name, it does not pin the bin's container type (unlike a
list selector or map-key segment). Pin the bin explicitly — `$.bin:LIST.*…` or `$.bin:MAP.*…` (§18).

### 4.4 Key list with filter chain
Restrict a map to specific keys, then filter those entries at the current level:

```
$.map.{@"key1","key2","key3"}&[?(predicate)]
```

`&[?(` must be contiguous (no spaces between `&`, `[`, and `?`; no extra `.` before `&`).

### 4.5 Field projection after wildcard
After `.*` or `.*[?(…)]`, select a named field on each matched child with `.fieldName`.

## 5. Map selectors `{…}`
First character after `{` sets the dimension: (none) = index, `@` = key, `=` = value, `#` = rank.

Selector operands are static literals only (§4.2) — not parenthesised expressions. Operand types by
dimension:

| Dimension | Operand types |
|---|---|
| Key (@…) | INT, STRING, BLOB literals, NIL, INF |
| Value (=…) | Any scalar literal: INT, FLOAT, STRING, BOOL, BLOB, NIL, INF |
| Index ({n}) | INT only — a BLOB or other non-integer literal is a parse error |
| Rank (#…) | INT only — a BLOB or other non-integer literal is a parse error |

Examples with BLOB keys: `$.perms.{@x'dead'}`, `$.caps.{@x'aa':x'ff'}`, `$.caps.{@x'aa',x'bb'}`,
`$.scores.{=x'cafe'}`.

In selector tables, `—` means that form is not available for that dimension.

| Dimension | Singular | Range | Open-start | Open-end | List | Inverted range | Inverted list |
|---|---|---|---|---|---|---|---|
| Index | `{1}` | `{1:5}` | `{:5}` | `{1:}` | — | `{!1:5}` | — |
| Key | `key` or `{@key}` | `{@x'ab':x'def0'}` | `{@:d}` | `{@a:}` | `{@a,b,c}` | `{!@a:d}` | `{!@a,b,c}` |
| Value | `{=a}` | `{=a:d}` | `{=:d}` | `{=a:}` | `{=a,b,c}` | `{!=a:d}` | `{!=1,2,3}` |
| Rank | `{#1}` | `{#1:5}` | `{#:5}` | `{#1:}` | — | `{!#1:5}` | — |

**Relative (map):**

| Form | Meaning |
|---|---|
| `{#-1:1~ref}` | Rank-relative range |
| `{#-2:~ref}` | Rank-relative open end |
| `{!#-1:~ref}` | Inverted rank-relative |
| `{0:1~key}` | Index range relative to key |
| `{0:~key}` | Index open end relative to key |
| `{!0:1~key}` | Inverted index-relative range |

**IMPORTANT for Leaderboard-style "N entries either side of a known key" reads**: the relative
forms `{start:count~key}` give an index range *relative to a known key's position* directly, in one
selector — no separate index-lookup + computed-bounds `let` needed. E.g. to get up to N entries
before and N+1 from a key onward: `{-N:(2N+1)~'<mapKey>'}` (exact clamping/boundary behavior at the
start/end of the map needs to be verified empirically — the AEL doc doesn't state whether it
silently clamps or requires the caller to pre-clamp).

Trailing comma: `{@k,}` is multi-select with one key (invertible as `{!@k,}`). `{@k}` alone is
singular (0 or 1 element). The same convention applies to the value dimension: `{=a,}` / `{!=a,}`
is a one-element value list; `{=a}` alone is singular.

Intervals: index and rank ranges use begin-inclusive, end-exclusive semantics.

## 6. List selectors `[…]`
After `[`, if the next non-whitespace character is `=` the selector is value dimension; if `#` then
rank; if `!` then inverted (re-parse remainder); otherwise index.

Selector operands follow the same rules as map selectors (§5). In selector tables, `—` means that
form is not available for that dimension (see §5).

| Dimension | Singular | Range | Open-start | Open-end | List | Inverted range | Inverted list |
|---|---|---|---|---|---|---|---|
| Index | `[1]` | `[1:5]` | `[:5]` | `[1:]` | — | `[!1:5]` | — |
| Value | `[=a]` | `[=a:d]` | `[=:d]` | `[=a:]` | `[=a,b,c]` | `[!=a:d]` | `[!=a,b,c]` |
| Rank | `[#1]` | `[#1:5]` | `[#:5]` | `[#-3:]` | — | `[!#1:5]` | — |

**Relative (list):**

| Form | Meaning |
|---|---|
| `[#-3:-1~ref]` | Rank-relative range |
| `[#-2:~ref]` | Rank-relative open end |
| `[!#-3:-1~ref]` | Inverted rank-relative |

Trailing comma (value dimension): `[=a,]` is multi-select with one value (invertible as `[!=a,]`).
`[=a]` alone is singular.

## 7. Loop variables
Valid only inside `*[?(…)]` filter predicates and `.modify(…)` bodies. Requires an enclosing `*`
wildcard in scope.

| Form | Meaning |
|---|---|
| `@` | Current iteration element value (eg the map value if iterating over map keys) |
| `@.field` | Navigate into current element (map key) |
| `@.[n]` | List index within current element |
| `@key` | Parent map key (metadata; no dot) |
| `@index` | Parent list index (metadata; no dot) |

Nested sub-expressions inside filter arguments are not allowed (no filter nested inside another
filter's path argument). Each `*[?(…)]` level has its own `@` scope.

## 8. Operators

### 8.1 Comparison
| Operator | Operand types | Result |
|---|---|---|
| `==`, `!=`, `<`, `<=`, `>`, `>=` | Same type both sides | TRILEAN |
| `expr in listExpr` | Right side evaluates to LIST | TRILEAN |
| `stringExpr =~ /pattern/[/flags]` | Left side STRING; ICU regex | TRILEAN |

The `=~` operator is the regex match form in AEL (§2.9). The pattern must appear as a regex literal
in the expression (`/…/` or `/…/flags`); it cannot be taken from a bin or variable.

Comparisons are not chainable (`a < b < c` is a parse error). Literals may appear on either side.

`in`: the right-hand side must evaluate to a LIST; the operator tests whether the left-hand value
equals any list element. It does not test map keys or map values directly — use selectors or
`exists()` (§22.3). The left operand's type must be resolved by the ordinary strict-typing rules
(§18) — e.g. `$.x in $.other` with neither side pinned is a parse error.

### 8.2 Logical
| Form | Operands | Result |
|---|---|---|
| `a and b` | TRILEAN | TRILEAN |
| `a or b` | TRILEAN | TRILEAN |
| `not(expr)` | TRILEAN | TRILEAN |
| `exclusive(a, b, …)` | TRILEAN (varargs) | TRILEAN — true iff exactly one operand is true |

`and` binds tighter than `or`.

### 8.3 Arithmetic and string concatenation
| Operator | Operands | Result | Notes |
|---|---|---|---|
| `+` | INT or FLOAT (matching) | Same numeric type | Numeric addition |
| `+` | STRING (matching) | STRING | String concatenation |
| `-` | INT or FLOAT (matching) | Same numeric type | |
| `*`, `/`, `%` | INT or FLOAT (matching) | Same numeric type | % integer only |
| `**` | FLOAT | FLOAT | Right-associative |

Type-directed `+`: When both are numeric, `+` adds. When both operands are STRING, `+` concatenates
(`$.first + ' ' + $.last`). A string literal anywhere in a `+` chain pins the chain to STRING; an
all-bin string chain with no anchor is a parse error. Mixing incompatible types is a type error.
There is no standalone `concat()`, `append()`, or `prepend()` string method; use `+` or `splice()`
(§14.2).

### 8.4 Integer bitwise
| Operator | Operands | Result |
|---|---|---|
| `&`, `^`, `\|` | INT | INT |
| `~expr` | INT | INT |
| `<<`, `>>`, `>>>` | INT | INT |

`>>` is arithmetic shift; `>>>` is logical (zero-fill). Precedence among `&`, `^`, `\|`: `&` tightest,
then `^`, then `\|`. Integer bitwise operators apply to whole 64-bit integers, not BLOB bit ranges.

### 8.5 Precedence (lowest to highest)
| Level | Operators / forms |
|---|---|
| 1 | or |
| 2 | and |
| 3 | ==, !=, <, <=, >, >=, in, =~ |
| 4 | \| (bitwise OR) |
| 5 | ^ (bitwise XOR) |
| 6 | & (bitwise AND) |
| 7 | <<, >>, >>> |
| 8 | +, - |
| 9 | *, /, % |
| 10 | ** |
| 11 | Unary ~, not(…), unary -, unary + |
| 12 | Primary: literals, paths, calls, (…) |

## 9. Control flow

### 9.1 Conditional: `when`
```
when (cond1 => val1, cond2 => val2, default => val3)
```
| Part | Requirement |
|---|---|
| Conditions (condN) | TRILEAN |
| Actions (valN) | All branches the same type, except the reserved literals unknown and error |
| default => … | Required fallback branch |

Result type is the unified action type of all non-unknown / non-error branches.

Reserved trilean value literals `unknown` and `error` are value literals — not exceptions, not
parse failures, and not shorthand for "throw". Both spellings are synonymous: they may appear on
any branch regardless of the types on other branches, and at evaluation time both produce the
TRILEAN value unknown. Typical use: `default => unknown` when no condition matches and the
expression should yield an indeterminate result rather than a typed default.

```
/* Parse error — branch result types must agree (INT default vs STRING arms) */
when ($.tier == 1 => 'gold',
      $.tier == 2 => 'silver',
      $.tier == 3 => 'bronze',
      default => 0
)
/* Valid — `unknown` and `error` are interchangeable value literals */
when ($.tier == 1 => 'gold',
      $.tier == 2 => 'silver',
      $.tier == 3 => 'bronze',
      default => unknown   /* same runtime value as `default => error` */
)
```

### 9.2 Local bindings: `let`
```
let (var1 = expr1, var2 = expr2) then (bodyExpr)
```
References to bound variables in bodyExpr use `${varName}`. Variable types are inferred from their
initializer expressions.

```
let (total = $.price * $.qty) then (${total} > 1000)
```

## 10. Record metadata functions
All forms are invoked on `$` with `()`.

| Function | Parameters | Return | Description |
|---|---|---|---|
| `$.ttl()` | — | INT | Time to live (seconds) |
| `$.voidTime()` | — | INT | Void time |
| `$.lastUpdateTime()` | — | INT | Last update time |
| `$.timeSinceLastUpdate()` | — | INT | Time since last update |
| `$.recordSize()` | — | INT | Record size (introduced server v7.0.0) |
| `$.deviceSize()` | — | INT | Device size — deprecated server v7.0.0; use recordSize() |
| `$.memorySize()` | — | INT | Memory size — deprecated server v7.0.0; use recordSize() |
| `$.keyExists()` | — | BOOL | Whether record key exists |
| `$.isTombstone()` | — | BOOL | Tombstone flag |
| `$.setName()` | — | STRING | Set name |
| `$.key()` | — | INT, STRING, or BLOB | Record primary key / digest; optional `:TYPE` after () |
| `$.digestModulo(n)` | INT | INT | Digest modulo (e.g. filter sharding) |

Bin presence and particle type use path terminals on a bare bin only — see §12
(`$.bin.exists()`, `$.bin.type()`). There is no record-level `$.exists()` or `$.type()` on `$`
alone.

Chaining after metadata calls. A metadata call such as `$.ttl()` is parsed as a complete
expression — it cannot be written as `$.ttl().toString()` (parse error). To apply a string or
conversion method to the result, wrap the call: `($.ttl()).toString()` (§4.2).

## 11. Standalone functions
Functions with no path receiver.

### 11.1 Numeric
| Function | Parameters | Return | Description |
|---|---|---|---|
| `abs(x)` | numeric | same as x | Absolute value |
| `ceil(x)` | FLOAT | FLOAT | Ceiling |
| `floor(x)` | FLOAT | FLOAT | Floor |
| `log(value:, base:)` | FLOAT, FLOAT | FLOAT | Logarithm |
| `pow(base:, exponent:)` | FLOAT, FLOAT | FLOAT | Power |
| `min(a, b, …)` | numeric (varargs, same type) | same as args | Minimum — positional |
| `max(a, b, …)` | numeric (varargs, same type) | same as args | Maximum — positional |
| `countOneBits(x)` | INT | INT | Count one bits in integer |
| `findBitLeft(x:, value:)` | INT, TRILEAN | INT | Find bit scanning left — value: true = set bit, false = clear bit |
| `findBitRight(x:, value:)` | INT, TRILEAN | INT | Find bit scanning right |

### 11.2 Logical
| Function | Parameters | Return | Description |
|---|---|---|---|
| `exclusive(a, b, …)` | TRILEAN (varargs) | TRILEAN | Exactly one true |

### 11.3 Geo
| Function | Parameters | Return | Description |
|---|---|---|---|
| `geoJson('…')` | STRING (JSON) | GEO | Constructs a GEO value from a GeoJSON string literal (not a dynamic expression) |
| `geoCompare(a, b)` | GEO, GEO | TRILEAN | Tests bidirectional spatial containment. Positional |

`geoJson`: the argument must be a string literal at parse time. Dynamic forms such as
`geoJson($.str)` are not supported.

## 12. Path read terminals
End a path after navigation/selectors/iteration. If no terminal is written, implicit get returns
matched values.

| Terminal | Parameters | Return | Description |
|---|---|---|---|
| (implicit get) | — | scalar or LIST | Default: values at path. Multi-select → flat LIST of values |
| `getKeys()` | — | LIST | Keys of matched elements (multi-select required) |
| `getKeyValues()` | — | LIST | Flat [k,v,k,v,…] — not a MAP |
| `getMaps()` | — | MAP | Key → value map (key-ordered default) |
| `getTree()` | — | MAP | Structure-preserving tree |
| `getIndexes()` | — | INT or LIST | Index(es) of matched elements |
| `getRanks()` | — | INT or LIST | Rank(s) of matched elements |
| `count()` | — | INT | Element count on single-select LIST/MAP path; match count on multi-select |
| `exists()` | — | TRILEAN | Whether path/value exists |
| `toInt()` | — | INT | Cast STRING or FLOAT value to INT |
| `toFloat()` | — | FLOAT | Cast STRING or INT value to FLOAT |
| `type()` | — | INT | Runtime particle-type code — bare bin only (`$.bin.type()`) |

Restrictions:
- `getKeys()` / `getKeyValues()` on a bare single-key navigation without multi-select context is a
  parse error.
- `getIndexes()` / `getRanks()` / `getMaps()` are not valid on wildcard, filter, or inner-multi-
  select paths (deferred — e.g. `$.m.*.x.getRanks()`).
- `getMaps()` requires a leaf map range or list selector; a list-only selector has no keys to
  return as a map (parse error).
- `type()` and bare-bin `exists()` apply to `$.bin` only, not nested paths
  (`$.bin.seg.type()` is a parse error).
- `toInt()` / `toFloat()` require the receiver type to be known at parse time. If the bin is not
  already pinned elsewhere in the expression, attach `:INT`, `:FLOAT`, or `:STRING` on the path
  before the call (§3.3). For example: `$.bin:STRING.toInt() > 12`.

Result shapes (multi-select):

| Terminal | Shape |
|---|---|
| Implicit get | Flat list of values |
| `getKeyValues()` | Flat alternating keys and values |
| `getMaps()` | Single map (key → value) |
| `getTree()` | Nested map preserving path structure |

## 13. Path write terminals
Return the modified collection unless noted.

### 13.1 Map writes
| Function | Parameters | Return | Description |
|---|---|---|---|
| `setTo(value)` | any | MAP | Upsert key from path navigation |
| `update(value)` | any | MAP | Update only — fail if key missing (map only) |
| `insert(value)` | any | MAP | Create only — fail if key exists (map only) |
| `add(amount)` | numeric | MAP | Numeric delta on map value |
| `putItems(items)` | MAP | MAP | Bulk upsert map entries from items; optional `:PARTIAL` (§17) |
| `insertItems(items)` | MAP | MAP | Bulk create-only — fail if any key exists; optional `:PARTIAL` |
| `updateItems(items)` | MAP | MAP | Bulk update-only — fail if any key missing; optional `:PARTIAL` |
| `remove()` | — | MAP | Remove matched entries |
| `clear()` | — | MAP | Clear entire map |

Bulk symmetry: map `putItems` / `insertItems` / `updateItems` mirror single-key `setTo` / `insert` /
`update`.

### 13.2 List writes
| Function | Parameters | Return | Description |
|---|---|---|---|
| `append(value)` | any | LIST | Append to end |
| `appendItems(items)` | LIST | LIST | Bulk append elements from items; optional `:PARTIAL` |
| `insert(value)` | any | LIST | Insert at index from path |
| `insertItems(items)` | LIST | LIST | Bulk insert elements at index from path navigation; optional `:PARTIAL` |
| `setTo(value)` | any | LIST | Overwrite at index |
| `add(amount)` | numeric | LIST | Increment at index |
| `remove()` | — | LIST | Remove matched elements |
| `clear()` | — | LIST | Clear entire list |
| `sort()` | — | LIST | Sort list (zero arguments); optional `:DROP_DUPS` |

List write terminals also accept `:ADD_UNIQUE` (§17) and `:NO_FAIL` where applicable.

Bulk symmetry: list `appendItems` / `insertItems` mirror `append` / `insert`. List `setTo` / `add`
at a path index mirror map single-key `setTo` / `add` (lists have no map-style update).

Path addressing (maps vs lists): write verbs split where goes in the path from what goes in `()`.
Maps are keyed — the path names one key (or the whole map for bulk); bulk keys and values live in
the map argument. Lists are indexed — `insert`, `insertItems`, `setTo`, and `add` take the index
from path navigation (e.g. `$.l.[i].…`); the argument holds value(s) only. The index is never a
function parameter.

| | Address in path | Payload in () |
|---|---|---|
| Map single-key | key (e.g. `$.m.k.…`) | value / amount |
| Map bulk | whole map (`$.m.…`) | map of keys → values |
| List at index | index (e.g. `$.l.[i].…`) | value / list of values |
| List bulk append | whole list (`$.l.…`) | list of values |

`insertItems` overload: same verb, different path shape — `$.m.insertItems({…})` is whole-map bulk
create (no index leaf); `$.l.[0].insertItems([…])` is positional bulk insert at the path index,
analogous to `$.l.[0].insert(value)`. `appendItems` is the list bulk op at container scope (append
at end; no index leaf), parallel to map bulk ops on `$.m.…`.

Name disambiguation: `append(value)` on a LIST receiver appends a list element. String
concatenation uses the `+` operator, not a string method.

### 13.3 Path modify and remove
| Function | Parameters | Return | Description |
|---|---|---|---|
| `modify(expr)` | expression using @ | same as source bin | Transform each matched element; result replaces element |
| `remove()` | — | same as source bin | Delete matched elements; no-op if none match |

Inverted selection uses `!` prefix on selectors (e.g. `{!a:c}.remove()` removes entries outside the
range).

Create-order suffixes: `modify()` and `remove()` never accept create-order flags (§3.4 rule 3). For
absent-path tolerance on those terminals, use `:NO_FAIL` (§17).

Selector placement: `{…}` / `[…]` selectors attach to the path before the terminal — e.g.
`$.m.{@k}.remove()`, not `$.m.remove({@k})`. Terminal-argument selector forms (sometimes called a
"bridge") are not part of AEL; dynamic selection in selectors waits for path-parameter `(expr)`
(§4.2).

## 14. String path functions
Method-style on a STRING receiver may be a bin root (`$.str.…`), a nested/pathed string value
(`$.m.x.…`), or a chained string result. Positions and lengths are Unicode code points.

`toInt()` and `toFloat()` share names with path read terminals (§12). On bins whose type is not
already resolved, pin `:STRING` before the call — e.g. `$.code:STRING.toInt()` — so the compiler
selects string parsing rather than numeric cast.

String modify terminals and `:NO_FAIL`. Functions that return a new STRING and modify the
underlying value (`upper`, `lower`, `trim`, `splice`, `overwrite`, … — §14.1 transform rows and all
of §14.2) are write terminals when used on a pathed receiver (`$.m.x.…`). On such a path they
accept `:NO_FAIL` per §17: if a CDT context segment on the path is absent in the bin, the write is
a no-op and the original bin is left unchanged. Read terminals (`strlen`, `find`, `substr`,
`charAt`, `toInt`, …) do not accept `:NO_FAIL`. The flag is valid only on a pathed string modify —
not on a bare bin (`$.str.upper()`) or a parenthesised value receiver (`(expr).upper()`), where
there is no multi-segment CDT context. `:NO_FAIL` does not suppress parse errors, invalid flag
placement, or op-specific failures such as `overwrite` with offset past end.

### 14.1 Read and transform
| Function | Parameters | Return | Description |
|---|---|---|---|
| `strlen()` | — | INT | Character count |
| `substr(from: [, to:])` | INT [, INT] | STRING | Substring; from inclusive; to exclusive if present |
| `charAt(index:)` | INT | STRING | Single Unicode codepoint at index |
| `upper()` | — | STRING | Uppercase |
| `lower()` | — | STRING | Lowercase |
| `caseFold()` | — | STRING | Unicode case fold |
| `normalizeNFC()` | — | STRING | NFC normalization |
| `trim()` | — | STRING | Trim Unicode whitespace both ends |
| `trimStart()` | — | STRING | Trim leading whitespace |
| `trimEnd()` | — | STRING | Trim trailing whitespace |
| `find(needle:, occurrence:)` | STRING, INT | INT | Position of nth occurrence; -1 if not found |
| `contains(needle:)` | STRING | TRILEAN | Substring test |
| `padStart(length:, pad:)` | INT, STRING | STRING | Minimum length |
| `padEnd(length:, pad:)` | INT, STRING | STRING | Minimum length |
| `toInt()` | — | INT | Parse numeric string |
| `toFloat()` | — | FLOAT | Parse numeric string |
| `regexReplace(pattern:, replace:)` | REGEX, STRING | STRING | ICU regex replace all matches |
| `startsWith(prefix)` | STRING | TRILEAN | Prefix test |
| `endsWith(suffix)` | STRING | TRILEAN | Suffix test |
| `split(separator)` | STRING | LIST | Split to list of strings |
| `repeat(count)` | INT | STRING | Repeat string |
| `isUpper()` | — | TRILEAN | All characters uppercase |
| `isLower()` | — | TRILEAN | All characters lowercase |
| `isNumeric()` | — | TRILEAN | Numeric string test |
| `bytesLength()` | — | INT | Length in bytes |
| `toBlob()` | — | BLOB | String to blob |
| `b64Decode()` | — | BLOB | Base64 string to blob; fails on invalid base64 |

### 14.2 Modify (return new string)
| Function | Parameters | Return | Description |
|---|---|---|---|
| `splice(offset:, value:)` | INT, STRING | STRING | Insert at offset |
| `overwrite(offset:, value:)` | INT, STRING | STRING | Overwrite at offset; offset past end → error |
| `snip(from: [, to:])` | INT [, INT] | STRING | Remove range |
| `replace(find:, replace:)` | STRING, STRING | STRING | First occurrence |
| `replaceAll(find:, replace:)` | STRING, STRING | STRING | All occurrences |

### 14.3 Cross-type string conversions
| Function | Receiver type | Return | Description |
|---|---|---|---|
| `toString()` | INT, FLOAT, BOOL, STRING, BLOB | STRING | Format as string |

On a bin path, pin or infer the receiver type before calling — e.g. `$.amount:INT.toString()`.
Record metadata uses the parenthesis rule in §10 / §4.2 — e.g. `($.recordSize()).toString()`.

### 14.4 List string function
| Function | Parameters | Return | Description |
|---|---|---|---|
| `$.list.join(separator)` | STRING | STRING | Join list elements with separator |

### 14.5 Chaining
String methods that return STRING may chain left-to-right (`$.email.trim().lower()`). A method
that returns INT or TRILEAN ends the string-method chain — no further `.stringMethod()` may follow
it.

```
/* Valid — each call returns STRING */
$.sku.trim().upper().replace(find: '-', replace: '_')
/* Parse error — find() returns INT; .replace() cannot follow */
$.sku.find(needle: '-').replace(find: '_', replace: '.')
/* Valid — INT result used in an expression, not chained to another string method */
$.sku.find(needle: '-', occurrence: 1) == 3
```

## 15. BLOB (bit) path functions
Method-style on a BLOB receiver. Offsets and sizes are in bits unless noted as byte offset.

### 15.1 Read
| Function | Parameters | Return | Description |
|---|---|---|---|
| `bitGet(offset:, size:)` | INT, INT | BLOB | Extract bit range |
| `b64Encode()` | — | STRING | Base64-encode the blob |
| `bitCount(offset:, size:)` | INT, INT | INT | Count set bits in range |
| `bitLscan(offset:, size:, value:)` | INT, INT, TRILEAN | INT | Scan left for bit value |
| `bitRscan(offset:, size:, value:)` | INT, INT, TRILEAN | INT | Scan right for bit value |
| `bitGetInt(offset:, size: [, signed:])` | INT, INT [, BOOL] | INT | Extract as integer |

### 15.2 Modify (return modified BLOB)
| Function | Parameters | Return | Description |
|---|---|---|---|
| `bitResize(byteSize:)` | INT | BLOB | Resize to byte length |
| `bitInsert(byteOffset:, value:)` | INT, BLOB | BLOB | Insert bytes |
| `bitRemove(byteOffset:, byteSize:)` | INT, INT | BLOB | Remove bytes |
| `bitSet(offset:, size:, value:)` | INT, INT, BLOB | BLOB | Set bit range |
| `bitOr(offset:, size:, value:)` | INT, INT, BLOB | BLOB | OR on range |
| `bitXor(offset:, size:, value:)` | INT, INT, BLOB | BLOB | XOR on range |
| `bitAnd(offset:, size:, value:)` | INT, INT, BLOB | BLOB | AND on range |
| `bitNot(offset:, size:)` | INT, INT | BLOB | NOT on range |
| `bitLshift(offset:, size:, shift:)` | INT, INT, INT | BLOB | Left shift range |
| `bitRshift(offset:, size:, shift:)` | INT, INT, INT | BLOB | Right shift range |
| `bitAdd(offset:, size:, value: [, signed:])` | INT, INT, INT [, BOOL] | BLOB | Add in range; overflow fails |
| `bitSubtract(offset:, size:, value: [, signed:])` | INT, INT, INT [, BOOL] | BLOB | Subtract in range |
| `bitSetInt(offset:, size:, value:)` | INT, INT, INT | BLOB | Write integer in range |

Write-policy postfix flags: bit modify ops accept `:CREATE_ONLY`, `:UPDATE_ONLY`, `:NO_FAIL`, and
`:PARTIAL` where listed (§17). Bit read ops reject all write-policy flags.

`:CREATE_ONLY` and `:UPDATE_ONLY` are mutually exclusive. `:PARTIAL` on BLOB bit ops does not imply
`:NO_FAIL`.

| Flag | Effect |
|---|---|
| (default) | Upsert — create the bin or modify an existing blob as the op requires |
| `:CREATE_ONLY` | Fail if the bin already exists |
| `:UPDATE_ONLY` | Fail if the bin does not exist |
| `:NO_FAIL` | On a create/update/type conflict, succeed as a no-op instead of failing |
| `:PARTIAL` | Clip the op to the end of the blob when the range extends past the end; does not imply `:NO_FAIL` |

Valid flags by op:

| Op | :CREATE_ONLY | :UPDATE_ONLY | :NO_FAIL | :PARTIAL |
|---|---|---|---|---|
| bitResize | ✓ | ✓ | ✓ | — |
| bitInsert | ✓ | ✓ | ✓ | — |
| bitRemove | — | ✓ | ✓ | ✓ |
| bitSet, bitOr, bitXor, bitAnd, bitNot | — | ✓ | ✓ | ✓ |
| bitLshift, bitRshift | — | ✓ | ✓ | ✓ |
| bitAdd, bitSubtract, bitSetInt | — | ✓ | ✓ | — |

Only `bitResize` and `bitInsert` can create a missing bin; `:CREATE_ONLY` applies only to those two.

```
$.header.bitResize(byteSize: 4):CREATE_ONLY
$.header.bitInsert(byteOffset: 0, value: x'01'):UPDATE_ONLY
$.header.bitSet(offset: 8, size: 8, value: x'01'):UPDATE_ONLY:NO_FAIL
$.header.bitRemove(byteOffset: 0, byteSize: 2):PARTIAL
```

## 16. HLL path functions
Method-style on an HLL bin receiver. HLL modify operations (`hllInit`, `hllAdd`) require a
bin-direct receiver (not a nested path into an HLL value).

### 16.1 Read
| Function | Parameters | Return | Description |
|---|---|---|---|
| `hllCount()` | — | INT | Estimated cardinality |
| `hllDescribe()` | — | LIST | [indexBitCount, minHashBitCount] |
| `hllMayContain(list)` | LIST | INT | Probabilistic membership (1 or 0) |
| `hllUnion(other)` | single HLL bin path | HLL | Union with another HLL |
| `hllUnionCount(other)` | single HLL bin path | INT | Estimated union count |
| `hllIntersectCount(other)` | single HLL bin path | INT | Estimated intersection count |
| `hllSimilarity(other)` | single HLL bin path | FLOAT | Jaccard similarity 0.0–1.0 |

For `hllUnion`, `hllUnionCount`, `hllIntersectCount`, and `hllSimilarity`, pass `other` as a single
HLL bin path (e.g. `$.peerHll`). List literals are static only — bin paths inside `[…]` are not
valid.

```
$.hbin.hllSimilarity($.otherHll) >= 0.5
$.hbin.hllUnionCount($.peerHll) > 1000
$.hbin.hllIntersectCount($.peerHll) > 0
$.sketch.hllUnion($.otherSketch)
$.visitors.hllMayContain(['u1', 'u2']) == 1
```

### 16.2 Modify
| Function | Parameters | Return | Description |
|---|---|---|---|
| `hllInit(indexBits: [, minHashBits:])` | INT [, INT] | HLL | Create or reset HLL |
| `hllAdd(list [, indexBits: [, minHashBits:]])` | LIST [, INT [, INT]] | HLL | Add values |

`hllInit` accepts `:CREATE_ONLY`, `:UPDATE_ONLY`, `:NO_FAIL`; `hllAdd` accepts `:CREATE_ONLY` and
`:NO_FAIL` only (`:UPDATE_ONLY` is a parse error on `hllAdd`).

```
$.visitors.hllInit(indexBits: 12):CREATE_ONLY
$.visitors.hllInit(indexBits: 12):UPDATE_ONLY
$.visitors.hllAdd(['u1', 'u2']):CREATE_ONLY
$.visitors.hllAdd(['u1']):NO_FAIL
```

## 17. Postfix flags
Attach immediately after `)` on path terminals (not as named parameters inside `()`).

| Flag | Valid on | Description |
|---|---|---|
| `:NO_FAIL` | CDT writes; modify(), remove(); pathed string modify; hllInit, hllAdd; all BLOB bit modify ops | Absent-path / policy tolerance |
| `:PARTIAL` | Map putItems, insertItems, updateItems; list appendItems, insertItems; BLOB bit ops | Bulk: apply entries that succeed even when others fail (implies `:NO_FAIL`). BLOB: clip to blob end |
| `:CREATE_ONLY` | hllInit, hllAdd; bitResize, bitInsert | Fail if the operation would modify an existing bin |
| `:UPDATE_ONLY` | hllInit; all BLOB bit modify ops | Fail if the operation would create a new bin |
| `:ADD_UNIQUE` | List append, appendItems, insert, insertItems, setTo, add | Fail (or skip under `:NO_FAIL`) when an element equals one already in the list |
| `:DROP_DUPS` | sort() | Drop duplicate elements while sorting |
| `:REVERSE` | getIndexes(), getRanks() | Reverse index/rank direction |
| `:UNORDERED` | getMaps() | Unordered return map shape on getMaps() only |
| `:PERSIST_INDEX` | Bin root only | Persist top-level map index on create |

**Terminal-kind rule:** create-order suffixes (§3.4) are write-only and allowed only on write
terminals that can create containers. `:NO_FAIL` is write-only. Read terminals take neither
create-order suffixes nor `:NO_FAIL`. Use `exists()` (§12) to test presence on reads instead.

**`:NO_FAIL` semantics** — two runtime axes, both narrower than "suppress any failure":

| Axis | Valid on | Effect when set |
|---|---|---|
| Path-level (absent CTX) | CDT writes; modify(), remove(); pathed string modify | A CDT context segment on the compiled path is missing in the bin → no-op; original bin unchanged |
| Bin-level (create/update policy) | hllInit, hllAdd; BLOB bit modify ops | A `:CREATE_ONLY` / `:UPDATE_ONLY` conflict on the bin → no-op instead of failing |

`:NO_FAIL` does not suppress parse errors, read-terminal failures, or op-specific failures unless a
separate mechanism applies.

```
$.m.{@a: d}.getMaps():UNORDERED
```

`:PARTIAL` and `:NO_FAIL`: on bulk CDT ops, `:PARTIAL` automatically applies `:NO_FAIL` at compile
time. On BLOB bit modify ops, `:PARTIAL` and `:NO_FAIL` are independent — both may be combined
explicitly (e.g. `bitSet(…):PARTIAL:NO_FAIL`).

## 18. Type inference (summary)
Every bin, variable, and sub-expression must resolve to a concrete type at parse time. Silent
default types are not applied.

| Mechanism | Rule |
|---|---|
| Literals | Pin unknown operands ('hi' → STRING, 10 → INT, etc.) |
| Operators | Constrain operand types (and/or/not → TRILEAN; + → matching numeric) |
| Functions | Parameter and return types per tables above |
| Path shape | List selectors → LIST bin; map keys → MAP bin; a wildcard segment pins nothing |
| Explicit suffix | `$.bin:INT`, `@:FLOAT`, `$.l.[0]:INT` |
| Cross-bin compare | Comparing two bins without a literal requires `:TYPE` on at least one side |

Conflicting types for the same bin or variable, unresolved unknown types, and wrong types for
operators are parse errors.

`toInt()` and `toFloat()` can be invoked on either numeric or string types. The method name alone
does not disambiguate — `$.amount.toFloat()` is a parse error unless `$.amount` was pinned earlier
in the expression. Use `$.amount:INT.toFloat()`, `$.amount:FLOAT.toInt()`, or
`$.amount:STRING.toFloat()` as appropriate.

## 19. Compile limits
| Limit | Value |
|---|---|
| Parser stack depth | 512 |
| Compiled expression size | 1 MiB |
| AEL source text size | 1 MiB |

## 20. Name collisions and disambiguation
| Name | Context A | Context B |
|---|---|---|
| insert | `$.l.[i].insert(value)` — list element | `$.str.splice(offset:, value:)` — string chars (named splice) |
| append | `$.l.append(value)` — list element | — (string concat uses +) |
| set | Map/list `setTo(value)` write terminal | `$.blob.bitSet(…)` — BLOB bit range (prefixed) |
| remove | CDT `remove()` terminal | `$.blob.bitRemove(…)` — BLOB bytes (prefixed) |
| count | Path `count()` — collection size/match count | Standalone `countOneBits(x)` — integer bit population |
| exists | Path `exists()` on nested values | Bare bin `$.bin.exists()` |
| type | Bare bin `$.bin.type()` → particle-type code | Type constants INT, STRING, … in comparisons |
| toInt / toFloat | Path read terminal — numeric cast on INT/FLOAT | String method — parse numeric text on STRING |
| ~ | Relative binding in selectors `{0:~key}` | Unary bitwise NOT on integers |
| * | Path wildcard segment | Literal wildcard in collections |
| @ | Loop variable in filters | Map key dimension `{@key}` in selectors |

Receiver type and argument arity distinguish colliding method names.

## 21. Quick reference — selector punctuation
| Token | In `{…}` / `[…]` |
|---|---|
| (none) | Index dimension |
| @ | Map key dimension (in `{…}` only) |
| = | Value dimension |
| # | Rank dimension |
| : | Range separator |
| , | List / multi-select |
| ~ | Relative-to binding |
| ! immediately after `{` or `[` | Inverted selection |

## 22. Examples

See the full doc for the exhaustive example set (§22.1–22.11). Key ones relevant to this repo:

```
/* Type suffix on a list index read — THE FIX for AdvancedExpressions' "counter" bin */
$.acc.[0]:INT

/* Local bindings */
let (total = $.price * $.qty) then (${total} > 1000)

/* Conditional */
when (
    $.tier == 1 => 'gold',
    $.tier == 2 => 'silver',
    default => 'bronze'
)

/* List membership */
'urgent' in $.tags

/* Map key range relative to a known key - for Leaderboard-style windowed reads */
$.scores.{0:1~key}          /* index range relative to key */
$.scores.{#-1:1~ref}        /* rank-relative range */
```
