# COMMS -- product-side integration checklist

This file documents how a product is wired to comms and what it may ask
of it. The comms repo never touches the product.

**None of it is a manual procedure.** comms is installed by Primer's
deploy machine, from its `primer.yaml` (section 15) and its compose file:
the machine renders the values, joins the networks and runs the
migration; a product takes comms by its own `primer.yaml`. This file is
the reference for WHAT happens and WHY. (Until W1-2 the wiring was the
old installer's, two passes of it run by each product's installer; it is
gone.)

Scope guard: the integration code -- the `notification_request` relay
into the comms stream, calls to the comms HTTP API, and the user-facing
proxy that sets `recipient_id`/`sender` server-side -- lives in the
product repo and is not part of this file.

---

## 1. How the product reaches comms

comms' compose file declares no network towards a product: the
machine's override joins them. `comms-app` is on the machine's network
as `comms-comms-app` -- the seam `COMMS_API_URL` is
`http://comms-comms-app:8000`; `comms-redis` is on the network of each
pair of the point `events` as `comms-comms-redis` -- the seam
`COMMS_EVENTS_ADDRESS` is `comms-comms-redis:6379`. Inside comms, its
own network is internal: the database is on it alone, with no way out of
the box, and no product reaches it.

The comms API is INTERNAL: no host port, no nginx route. Only a service
of the product the machine pairs with comms resolves these names.

## 2. The comms values the product takes

The product takes comms' seams (section 15) by name, from its own
`primer.yaml` (`source: seam, from: comms`), and builds what it reads
from them -- its stream URL, for one, from `COMMS_EVENTS_PASSWORD` and
`COMMS_EVENTS_ADDRESS`:

```
COMMS_API_URL=http://comms-comms-app:8000
COMMS_SERVICE_TOKEN=<minted by the machine -- single source>
COMMS_EVENTS_ADDRESS=comms-comms-redis:6379
COMMS_EVENTS_PASSWORD=<minted by the machine>
```

Semantics:

- `COMMS_SERVICE_TOKEN` -- the shared service token (trust seam,
  DD-6). The product backend presents it as `Authorization: Bearer
  <token>` on every call to the comms API. Minted ONCE by the machine;
  comms and the product carry the same value.
- `COMMS_API_URL` -- the comms API address on the machine's network.
- `COMMS_EVENTS_ADDRESS`, `COMMS_EVENTS_PASSWORD` -- the durable comms
  stream backbone and its password. The relay XADDs
  `notification_request` entries there. Treat the password with the
  same care as the rest of your secrets.

**Changing the service token without downtime.** comms accepts two
service tokens: `COMMS_SERVICE_TOKEN`, the current one, and
`COMMS_SERVICE_TOKEN_NEXT`, the next one. A request passes with either;
any other token is the `401`. comms is ready for a change of key in
three steps (Primer protocol 5.4): the next token is handed to comms
beside the current one, and comms, restarted, accepts both; the product
is moved onto the next token; the next token becomes the current one
and the next is emptied, and comms, restarted, accepts it alone. Primer's
machine does not change the key yet: its `primer.yaml` declares no
`COMMS_SERVICE_TOKEN_NEXT` (section 15), so on a box of the machine the
current token is the only one in force. The next token
is optional -- unset or empty, only the current one passes -- and never
stands alone: set without `COMMS_SERVICE_TOKEN`, comms refuses to start,
in development too. It may equal the current one. Neither token is ever
written to a log, and nothing comms logs says which of the two a
request passed with.

**The stream and group names are half of an agreement, so they are
written down on both sides.** The relay XADDs into the stream
`comms:events`; comms reads it as the consumer group `comms`. Both are
the defaults on the comms side (`COMMS_EVENTS_STREAM`,
`COMMS_CONSUMER_GROUP` in `deploy/.env.example`), so a deploy that
names neither works -- but a relay pointing at a different stream is
not an error anywhere: it writes where nobody reads, and the
notifications simply never arrive, with no failure to see on either
side. If a deploy overrides either name, every product writing to it
must be changed in the same step.

## 3. The product profile (`TEMPLATES_DIR`)

comms carries no product vocabulary: the notification types and their
templates are a per-product artifact (arch #15) that lives in the
PRODUCT repo -- for velo, `comms-profile/` in the repo root
(`types.yaml` + `templates/{locale}.yaml`).

A template sheet names a channel that renders it: `telegram` or
`email`. A sheet for `in_app` refuses the start, named: an in_app
delivery is the inbox entry, which shows the `title`/`body` the product
sent -- a template there would never be shown. Nor is there a sheet for
`llm`: its letter is the product's own.

**A variable the job does not carry is printed as its placeholder.** A
template field that names a variable absent from the job, a path into
one that does not resolve (`{user[name]}` with no `user` or no `name` in
it), or a variable that is `null` prints its path in braces --
`{paid_amount}`, `{user[name]}` -- where its value would stand; the rest
of the letter renders as usual, in every field: the telegram title,
body and button, the email subject and body. The job is accepted and
the letter goes: the channel's `accepted` row in the job's path names
the paths (`template_unresolved`, section 8). A type with no template
for a field still takes the stored title or body you sent -- your own
text, not a hole in one (see "What it can change" under
`TEMPLATES_OVERLAY_DIR` below, and "What email reads from the profile" in §3a).

**What a template field may not do.** The start refuses a template that
takes an attribute of a variable (`{user.name}`, `{x.__class__}` -- use
`{user[name]}`), puts a field inside a format spec (`{amount:{width}}`),
or asks a width or a precision above 4096 (`{x:99999999}`).

**What else the start refuses in the templates.** A text that is blank
(`""`, or spaces only) in `templates/{locale}.yaml` -- leave the field out
instead, and the letter takes your own title and body (the default label
for a button). A telegram title and body whose HTML Telegram would not
parse, checked together as they are sent, for a recipient of every
locale of the profile -- each field by the chain the send walks (the
recipient's locale, then `DEFAULT_LOCALE`), and a variable standing as a
value with a link's characters -- a tag Telegram does not support
(`<div>`, `<br>`), one left open or closed out of order, a raw `<`
(write `&lt;`), a variable in an unquoted attribute (`<a href={link}>`
-- quote it): every letter of the type would be refused. An entry
in `templates/` that is not a `<locale>.yaml` file (`en.yml`, a
directory); a hidden entry (`.gitkeep`, `.DS_Store`) is skipped. No
`templates/` at all is a profile whose letters come as
you write them; a directory beside `types.yaml` and `templates/` is only
logged (`profile_unknown_directory`) -- the start goes on.

It is delivered by BIND, not by a copy, read-only at `/profile` in the
three comms processes (`TEMPLATES_DIR=/profile`). **Temporarily, until
W4-1**, the bind is of the host path `/srv/comms/profile`: the machine
refuses a host path, so comms is installed by it nowhere until W4-1
brings the profile as data comms takes (`accepts` of `primer.yaml`).
Consequences that hold either way:

- editing a template is a commit in the product repo plus a roll-out;
  there is no second copy on the comms side to keep in step;
- comms validates the profile at startup and refuses to boot on a broken
  one, so a bad template commit can take the running comms stack down.
  Recovery is to revert that commit and roll out again.

### The type record -- every field

A type in `types.yaml` is a closed record: the fields below and keys in
the reserved `x-` namespace, nothing else -- any other key refuses the
start, named, with this list. Every field is optional; one not declared
takes the value in the third column. This table is the one place the
record is written down: the sections below and section 11 explain some
fields further, and do not repeat them.

| field | value | when not declared | what it does |
|---|---|---|---|
| `category` | a non-empty string, at most 50 characters -- the product's own key, never interpreted (not on an llm type) | none | the preference category: a recipient who muted it is not sent the type (section 6); a type without one is never muted |
| `channels` | a non-empty list of distinct names from `email`, `in_app`, `llm`, `telegram`; `llm` goes alone | `[in_app]` | where the type is delivered; a request never names a channel |
| `lane` | one of `interactive`, `critical`, `normal`, `bulk` | `normal` | the lane its jobs travel in (`lane`, below) |
| `push_on` | one of `outcome`, `outcome_and_deferral`, `none` | `none` | what comes back on the push stream (section 10) |
| `recipient_schedule` | one of `respect`, `bypass` (not on an llm type) | `respect` | whether its deliveries wait for the recipient's allowed periods (`recipient_schedule`, below) |
| `expires_after` | a duration: a whole number and `s`, `m`, `h` or `d` -- `30s`, `15m`, `24h`, `7d`; at most `3650d` | none | how long a request of the type stays deliverable; a request's own `expiry_at` wins (section 7) |
| `retry_max_attempts` | an integer, 1..100 | the deploy's `NOTIFICATION_MAX_DELIVERY_ATTEMPTS` | how many times one delivery is attempted -- a number, never a condition |
| `retry_backoff_seconds` | an integer, 0..86400, not above the deploy's `NOTIFICATION_RETRY_BACKOFF_MAX_SECONDS` | the deploy's `NOTIFICATION_RETRY_BACKOFF_BASE_SECONDS` | the first wait between attempts, in seconds (0 = none); the waits grow from it up to that cap |
| `max_body_chars` | an integer, 1..5000 (not on an llm type) | `5000` | the longest body a request of the type may carry; a longer one is refused at intake |
| `llm_sets` | a list of key set names defined in `LLM_KEY_SETS` (the llm kind only, required there) | none | the key sets the model call goes through, in order (section 11) |
| `answer_keep` | a duration, as `expires_after`, not longer than `NOTIFICATION_RETENTION_DAYS` (the llm kind only) | `24h` | how long the model's answer is kept after it came (section 11) |
| `max_audio_bytes` | an integer, 1..20971520 (the llm kind only) | `2097152` | the largest audio body a job may link to, in bytes as fetched (section 11) |

**A telegram letter has Telegram's limits.** `max_body_chars` bounds the
body a request carries; Telegram bounds the whole text it is sent -- the
title, the body and the template's own text together: at most 4096 characters
once its markup is read (an emoji is one character, a tag none, `&amp;`
one), at most 32768 bytes as sent, and not empty once read. A letter
certain to be past one of them is `failed`, `message_rejected` on its first
attempt, Telegram not asked; the delivery's text names the size and the
limit, never the letter. A letter Telegram itself refuses for the same
reasons is `failed`, `message_rejected` on that attempt too -- never
retried until it expires.

### `TEMPLATES_OVERLAY_DIR` -- the text overlay

One image and one profile can serve several installations whose
wording differs. The difference lives in a second, optional directory:
the **text overlay**. For comms it is a folder of texts and nothing
more -- comms knows nothing of whose installation it is, and does not
require it to cover every locale of the profile (that check is the
product's).

**Seam.** `TEMPLATES_OVERLAY_DIR` names the directory inside the
container; empty or absent -> no overlay, the profile's texts exactly as
they are. On a box it has no mount until W4-1, which brings it with the
profile: until then it is empty there, and the overlay is off.

**Form.** `{locale}.yaml` files directly in the directory -- no
`types.yaml`, no `templates/` -- each of the shape of
`templates/{locale}.yaml`:

    {type: {channel: {field: text}}}

Every leaf replaces exactly the profile's leaf at the same locale,
type, channel and field; every other text stays the profile's. One
field alone is fine: a title from the overlay over the profile's body.

**What it can change: the texts comms renders from a template, and
nothing else.** Today that is the telegram `title`, `body` and
`button_text`, and the email `subject` and `body` -- whichever of them
the profile itself has for that type, channel and locale. **in_app has
no template text**: the inbox shows the `title`/`body` the product
sent, and so do the channels whenever the profile has no template for
a field. Those stored texts are the product's; the overlay never
touches them.

**What it cannot change:** types, channels, lanes, schedules, retries
-- any field of a type record lives in `types.yaml` only. Nor can it
add a locale, a type, a channel or a field the profile does not have,
or a presentation flag (`disable_preview`, `silent`).

**Variables.** A text may use `{title}`, `{body}` and the variables
the profile's own templates of that type use in any locale and
channel. A type declares no variable list; the profile's texts are the
record of what the product sends for it.

**Startup refusals** -- comms does not start, and the log names the
file and the place, every problem at once:

- the directory does not exist, or is not a directory;
- an entry that is not a `{locale}.yaml` file (a subdirectory, `.yml`,
  a README, a dot-file);
- unparseable YAML; an empty file; a file, type or channel that is not
  a non-empty mapping; a key declared twice;
- a locale with no `templates/{locale}.yaml` in the profile; a type,
  channel or field the profile does not have there; the `in_app`
  channel; a field that is a presentation flag;
- a text that is not a string, is blank, fails the same checks as the
  profile's templates (format spec, `{user.name}`, a domain literal),
  or uses a variable the type does not have.

A broken overlay is never dropped in favour of the profile's text. An
existing **empty** directory is legal: zero overrides, and the startup
log says so with the path (`profile_overlay_installed`, `overrides=0`).

Example -- one type, one field, one locale (`en.yaml`):

    order_ready:
      telegram:
        title: "Your booking for {title} is confirmed"

### `lane` -- which lane a type's jobs travel in

comms delivers in four lanes, each with its own budget: its own
selection of due jobs, its own number of jobs per pass, its own number
of jobs in work at once, its own slots for channel calls, its own loop
in the worker. A backlog or a slow channel in one lane takes nothing
from another. A type declares its
lane in its record in `types.yaml`:

    lane: interactive

| value | for | the type's jobs | jobs in work at once | channel calls at once |
|---|---|---|---|---|
| `interactive` | someone waits for the answer right now (a login code) | small passes, a few slots of their own | 2 | 2 |
| `critical` | a failure costs money | capacity no other lane can take | 2 | 2 |
| `normal` | notifications | today's behavior -- the default, for a type that does not declare it | 4 | 20 |
| `bulk` | large sends | may wait; one slot, so a mass send never holds anyone else's | 1 | 1 |

What a lane does: it ISOLATES. A bulk backlog of any size does not delay
an `interactive` job by one pass, and a slow channel call in one lane
does not hold another lane's loop.

Inside a lane, several of its jobs are in work at once (the table's last
two columns). Each holds one of the lane's places to the end of its
attempt, and their channel calls share the lane's slots. A slow call
holds one place, not the lane: the lane's other jobs go on in the rest,
until every place is held by a slow call. `bulk` has one place: its jobs
go strictly one after another. Jobs of one lane are started oldest
first; those in work at once may end in any order -- two jobs of one
lane to one person may arrive in either order. Each worker names these
numbers in its startup line, `lane_budgets` (`jobs`, `concurrency`).

What a lane does NOT do. It is not a priority: no lane is picked before
another, each simply has its own. It passes no gate: a mute, the
recipient's schedule (unless the type declares `recipient_schedule:
bypass`, below), the retry rules and `expiry_at` behave as for any
type. And it does not make a channel faster: a slow provider is as slow
in `interactive` as anywhere -- only no longer at anyone else's cost.

A job is picked up as soon as it is accepted, in every lane: accepting
it wakes the loop of its lane in each worker that takes that lane, so an
idle worker does not hold it for its poll interval. The wake only speeds
things up: a lost one (the worker's listening connection down) falls
back to the poll -- up to `NOTIFICATION_MAX_BACKOFF_SECONDS` later -- and
never loses the job. A job that becomes due later (a `scheduled_at` in
the future, a retry) is picked up by the poll once due.

A lane outside the four refuses startup, like every other field. The
value is taken when the request is accepted: a job keeps the lane its
type had then, whatever a later profile says.

**Which worker takes which lane is the deploy's, not the profile's.**
One worker image takes the lanes named in `COMMS_WORKER_LANES`
(comma-separated, in the environment of the worker); left out, it takes all four --
nothing needs to be written for one worker. A second worker of the
same image may take a subset, e.g. `COMMS_WORKER_LANES=critical` next
to one with `interactive,normal,bulk`. **The worker processes of a
deploy together must take all four lanes:** a lane no process takes is
neither delivered nor expired -- its jobs stay pending. Nothing fails
on it: a worker that takes a subset starts, and says so in one warning
line, `worker_lanes_partial` (`lanes_not_taken`); the lanes' backlog
shows in `GET /api/v1/lanes/depth`. Each worker names its lanes and
their budgets in its startup log line, `notification_worker_started`
(`lanes`, `lane_budgets`). Written but empty, an unknown lane or a lane listed
twice refuses startup, naming the variable.

Each lane also has its own timeout for one channel call,
`NOTIFICATION_LANE_<LANE>_DELIVER_TIMEOUT_SECONDS` (30 by default, in
`lane_budgets` as `deliver_timeout`); it must be above the client
timeout of every channel live on the deploy, and a lane at or below
one refuses startup, naming the variable and the channel.

### `recipient_schedule` -- a type that does not wait for the schedule

A recipient's schedule (section 6) holds every delivery outside their
allowed periods until the next one opens. A type the person is waiting
for RIGHT NOW -- a login code, a password reset -- cannot wait for the
morning: its `expiry_at` runs out first and it never goes. Such a type
declares, in its record in `types.yaml`:

    recipient_schedule: bypass

| value | the type's deliveries |
|---|---|
| `respect` | wait for the recipient's next allowed period -- the default, for a type that does not declare it |
| `bypass` | go at any hour, the recipient's schedule notwithstanding |

What `bypass` does NOT do: it does not pass a mute. A recipient who
switched the type's category off gets nothing, at any hour. A type with
no category cannot be muted either way -- that is the category's rule,
not this field's. Nothing else changes: retries, a provider's "come back
later", `expiry_at` behave as for any type.

The value is taken when the request is accepted: a job keeps the one
its type had then, whatever a later profile says.

## 3a. How a product declares its channels

Every product has a different set of delivery channels, and a channel
it does not have is not a failure -- it is a design decision. One rule
expresses that, and it is the only mechanism:

**A channel is decided by its flag, and by nothing else.** Under the
Primer's machine a channel is an option of the instance: the passport
switches it on or off, and comms learns of it only by the flag. A
channel exists on a deploy when it has an implementation in this
service and its flag is `true`:

| the channel's flag | what it means | what happens |
|---|---|---|
| **`false`** | this product has no such channel on this instance, by design | the service starts; the channel is absent; its keys are **not read and not checked**, whatever they hold |
| **`true`**, every key set and well formed | the product has the channel | the channel is live |
| **`true`**, a key missing or malformed | an integrator typo | **the service refuses to start**, naming the channel and every such key |
| absent, or anything but `true` / `false` | an integrator typo | **the service refuses to start**, naming the flag |

### The channel flags

| channel | flag |
|---|---|
| `telegram` | `TELEGRAM_ENABLED` |
| `email` | `EMAIL_ENABLED` |
| `llm` | `LLM_ENABLED` |

The value is exactly `true` or `false`. Not `True`, not `1`, not `yes`;
and a flag has no default: an env without it does not start. `in_app`
has no flag: it is always on -- its delivery is the row the inbox reads,
nothing leaves the process, and there is nothing to configure or to
switch. A channel with no implementation in this service is absent on
every deploy, whatever the environment says. Under the machine the flag
comes from the passport (`options.<channel>`); the value the machine
writes into the keys of a switched-off option is never read.

The declared keys of every implemented channel live in one place,
`app/core/channels.py`. That file is the answer to "which keys does
this channel need"; this document does not duplicate the list.

**A request never names a channel.** The profile routes each type to
its channels (`types.<type>.channels`). A request that still carries
`channels` (or any other unknown field) is rejected at intake and
recorded under its key.

**A type routed into a switched-off channel is accepted, and its
delivery there is the outcome "channel unavailable".** The profile
loads; a job of the type is accepted; its delivery into the switched-off
channel is `failed`, class `configuration`, at once and without
retries, and says so in the log (`delivery_channel_unavailable`) and in
the delivery row, whose `error_message` names the flag:

    channel 'email' is not available on this deploy: not configured (EMAIL_ENABLED=false on this deploy)

The job's other channels go as usual: a job for `[in_app, email]` with
email off is `partial_sent`, its `in_app` delivery `sent`; a job whose
every channel is off is `failed`. It is never reported as a success. An
llm job with the llm channel off is accepted the same way -- its
`llm_audio` link included, which is then not checked against
`LLM_BODY_HOSTS` and never fetched -- and its one delivery is that
outcome. A product that cannot do without a channel guards it with its
own `options_required`; comms does not refuse the start for it. The
same outcome meets a job accepted while a channel was on, delivered
after a restart that switched it off.

**Startup refusals are readable.** The configuration is built before
logging is set up, so a refusal prints a message -- naming the channel
and the keys -- and exits, rather than a traceback.

**What the first start tells you.** The `comms_started` log line
carries a channel map: every channel with `live`, `not_configured` (the
flag is `false`) or `not_implemented`. The same map is served by
`GET /health` under `channels`, so confirming it later needs a request
rather than a grep through a container's log. That map is the way to
confirm that a deploy sees the channels its installer meant to give it.
Unknown variables are ignored on purpose, so that a leftover in a
shared env file cannot refuse a working deploy -- and a misspelt channel
variable is still caught: a misspelt flag is an absent flag, and a
misspelt key of a switched-on channel is a missing key, both refusals.
The map says what a deploy HAS; what its channels DID is the second
answer, "Channel health" in section 5.

**The llm channel, concretely,** is one key, `LLM_KEY_SETS`, holding
named key sets: section 11.

**Email, concretely.** The channel is decided by its flag like any
other, and its keys are declared in `app/core/channels.py`. With
`EMAIL_ENABLED=true` three of them are required -- the provider API
key, the provider domain, and the sender address. The sender has no default because it is a product fact; a
default would make an incomplete set look complete. It accepts either a
bare address or the display form `Name <address>`, since a signed
sender delivers better -- in the env file the display form only in
quotes (`EMAIL_FROM_ADDRESS="Comms <noreply@example.com>"`): the file is
read by `source`, and an unquoted `<` is a redirection there. A value
carrying a line break is refused at startup.

The provider region is different in kind: it has a default, so
"unset" and "set to the default" cannot be told apart, and it is
therefore never a missing key. Its VALUE is still checked whenever it is
spelled out with email on, against a closed set of the provider's
regions: a plausible-looking `eu-west` refuses startup instead of
failing every send later.

**What the key set cannot check.** A syntactically perfect set with a
wrong API key, or with a domain that is not verified with the provider,
starts cleanly and fails on every message. That is by construction: a
network check at startup would make the service's boot depend on the
provider being reachable. The loudness therefore sits at send time --
the first refusal the provider attributes to configuration rather than
to the message is logged as `email_channel_not_viable`, once, naming
the channel; the messages after it are ordinary per-message lines, so a
dead channel is a signal instead of a thousand identical entries.

**What the provider's answer means.** For each email, by the provider's
status: any `2xx` is its acceptance -- the delivery is `sent` (accepted is
not delivered; what happens after is the provider's). `401`, `402`, `403`,
and a `400` or `404` whose JSON message names the sender -- a domain the
provider does not have (`404`, "Domain not found"), one not verified, a
sender outside it -- are the channel: `failed`, `configuration`, logged as
above. `429` waits the provider's `Retry-After`, or is retried without
one; `408` and `5xx` are retried. A redirect (`3xx`) is the channel too:
`EMAIL_API_BASE_URL` is not the provider's API. Every other answer -- a
`400` on the recipient, a `404` that names no sender -- is this email:
`failed`, `message_rejected`.

**What email reads from the profile.** Its own fields under its own
channel: `{type: {email: {subject, body}}}`. Nothing new enters the
profile contract, and a profile that declares neither field stays
valid -- the subject falls back to the notification title, and, when
that is blank, to the type key, because an empty subject is filed as
spam. The body falls back to the stored body and never to another
channel's template, whose markup would arrive as raw characters.
Declaring email templates does not make a notification use the channel:
the channel list comes from the type's record in the profile
(`channels`), taken when the request is accepted, and from nowhere
else.

**Your own email.** A request may carry the email ready-made -- the
`email` block of `notification_request` (section 7): `subject`, `text`
and, if you want one, `html`. Then the provider gets exactly these:
the subject, the text and the HTML as you sent them, with the same
sender, the same single recipient and tracking off as every other
email. Nothing of the profile is used for that email -- neither the
`subject` and `body` templates of the email channel nor the job's
`title` and `body`, which stay the letter of the other channels. comms
rewrites nothing: no variables are filled in, nothing is escaped, cut
or cleaned, the HTML is not sanitized or checked. The subject is one
line: a subject with a line break (`\r` or `\n`) is rejected at intake
-- comms refuses it rather than clean it, for the email is yours; a tab
or two spaces in a row go as they are. The sender and the single
recipient are comms', whatever the block holds. Send the text always:
it is the version of the message for a client that shows no HTML, and
an HTML message without one is a common reason to be filed as spam. One
block per job -- every recipient of a group job gets the same email; a
person who muted the type's category gets none, as before. Without the
block the email is composed from the profile as described above, byte
for byte as before.

**One delivery path.** There is no SMTP fallback, deliberately: a
second path no deploy exercises rots from the first day. The
consequence is worth stating -- while the provider is unreachable, mail
does not go out at all. Delivery is retried, and products that send
codes re-send them on request.

**No tracking.** comms sends every message without the provider's
tracking: clicks and opens are switched off in each request, whatever
the domain's own tracking settings say, so links arrive as the product
wrote them. One thing a request cannot switch off: the unsubscribe
footer is a setting of the product's mail DOMAIN at the provider, with
no per-message option -- keep it off, or a footer with an unsubscribe
link lands in every message, a login code's included. Read the domain's
tracking settings as the running process sees them (nothing is
changed):

    docker exec comms-app python -c "import httpx; from app.core.config import settings as s; print(httpx.get(f'{s.email_api_base_url}/v3/domains/{s.email_mailgun_domain}/tracking', auth=('api', s.email_mailgun_api_key)).json()['tracking']['unsubscribe']['active'])"

It must print `False`.

**Time.** A product that mails short-lived codes should set `expiry_at`
on the notification to the life of the code. Delivery is retried on
transient failures, and when the provider asks to be retried later the
service honours the wait it names -- which can exceed the life of a
code by a wide margin. `expiry_at` is what stops a delivery that has
outlived its point; without it the service has no way to know.

**Scope of this rule: sending channels only.** A future inbound side
(receiving bot updates) is a separate capability with its own keys and
its own switch, and is not a channel of this registry. One bot has
exactly one update receiver, so a receiver switched on by the mere
presence of a sending token would start at a deploy that only asked to
send.

## 4. What the integration code does with all this

Consumers of the variables above are the integration track in the
product repo:

- outbox relay: velo -> `notification_request` into the comms stream
  (`COMMS_REDIS_URL`);
- server-to-server calls to the comms HTTP API (`COMMS_API_URL` +
  token);
- the user-facing proxy that sets `recipient_id`/`sender`
  **server-side** (comms authenticates the PRODUCT, not the user --
  the proxy owns user-level authorization);
- the push-stream listener (section 10), if the product wants to learn
  about a result without polling -- comms never calls the product back.

None of that is wired here; this file only accounts for the network
path, the credentials and the profile being in place before that code
runs.

## 5. The resource protocol (F1.4)

Every resource route -- recipients, threads, messages, sections, read
pointers, preferences, the inbox, reading a job by its key (section 8)
-- speaks the same language as a job.

**One form of refusal.** Every error response of every route has one
body, with a class a program can branch on:

    {"error": {"class": "<class>", "message": "<text>", "fields": [...]}}

| status | class | when |
|---|---|---|
| 400 | `bad_request` | the request could not be read -- a body that is not UTF-8 JSON, or one nested deeper than its JSON can be read on a route that takes a body model; any other client status this table does not list travels with this class too. Fix the request; repeating it changes nothing |
| 401 | `unauthorized` | the service token is missing or wrong -- asked first, on every path but `/health` and `/ready`, before anything of the request is read: without it an unknown route, another method, a body of any size or shape is this 401 too |
| 403 | `forbidden` | the actor has no role in this thread |
| 404 | `not_found` | the entity or the route does not exist (a route: with the token) |
| 405 | `method_not_allowed` | the route exists, the method does not (with the token) |
| 409 | `conflict` | a key or a version is taken by other content; a thread claimed by another operator, or changed by another call while it was being assigned or released (section 14) |
| 409 | `stale_snapshot` | a recipient snapshot or deletion older than the stored one |
| 409 | `recipient_deleted` | the recipient was deleted -- nothing re-attaches to it |
| 410 | `cursor_expired` | a changes cursor older than the retention period (section 9) |
| 413 | `bad_request` | the request's body is larger than 1048576 bytes (1 MiB) -- refused before it is read, with or without a `Content-Length`; the largest body the field limits allow is about 30 KB |
| 422 | `validation` | the input is wrong; `fields` names each input. A string in the body the database cannot store -- a NUL character or a lone surrogate, in a value or a key, at any depth -- is refused so, naming the top-level field and never its value; in the query string -- a NUL (`%00`) in a parameter's value or name, on every route -- so too, naming the parameter and never its value. A boolean or integer field takes that JSON type only: `true` is not an integer, `"1"` is not one either, `"no"` or `0` is not a boolean (the snapshot's `telegram_id` and `active`, a preference toggle, a thread's `priority`); a time that leaves the range once moved to UTC (`0001-01-01T00:00:00+05:00` as a read pointer) is refused, and so is a body nested deeper than its JSON can be read on a route that takes no body model -- on one that does, the framework answers `400` `bad_request` (above) |
| 500 | `internal` | our defect; the text carries nothing from it |
| 503 | `unavailable` | a transient failure of comms' database -- a deadlock, a lost or refused connection, a restart, an overload; with `Retry-After: 1`. The outcome is unknown: repeat the same request |

The message is for a human; the class is the contract.

**The request id.** Pass the `X-Request-ID` of your own request into
every call of comms, `/health` and `/ready` included (protocol 9.6).
comms keeps it when it is of the form `[A-Za-z0-9._-]{1,128}`; any
other -- absent, empty, longer, with a space or a line break -- is
replaced by an id comms mints. Of several `X-Request-ID` headers the
last is taken and judged alone: a malformed last one is replaced, never
an earlier one used. Every response carries the id the request was
served under in its own `X-Request-ID` -- the `401` of the token, the
`413` of the body and a `500` too -- and every log line comms writes
for the request names it in `request_id`.

**A write is answered after its commit.** Every route that writes
answers only once what it wrote is committed: a 2xx means the row is
there. A commit that fails is the answer -- a 503 or a 500 -- never a
success with nothing behind it.

**503 `unavailable`: the outcome is unknown -- repeat the same
request.** A connection lost on the commit itself may have taken the
commit with it or not; comms cannot tell, and neither can you. So
repeat the request as it was -- with the same `Idempotency-Key` where
the call takes one (`POST /threads`, `POST /threads/{id}/messages`, the
variant routes); every other write is safe to repeat by construction
(the table below). A repeat after a write that did land answers as the
first would have. A 503 that comes back every time is a defect of ours,
not a moment of the database: a misuse of a connection looks to comms
like a lost one, and is retried the same way until it is not.

**One way to page.** Every listing -- the inbox, the visible threads, a
thread's messages, a job's deliveries and its path, the address book,
the changes feed -- takes `limit` (1..100, default 20) and `cursor`
(the previous page's `next_cursor`, opaque) and answers
`{"items": [...], "next_cursor": "<opaque>" | null}`; the inbox adds its
`unread` badge beside them, and the changes feed's `next_cursor` is
never `null` (section 9). A `limit` outside 1..100 is **refused**
(422), not clamped, and so is a malformed cursor -- anything comms did
not hand out: a cursor of another listing, one whose time lost its
zone, one whose number is past the 64-bit range. The refusal names
the cursor, never its value.

**A query key given twice is refused**, on every route: 422
`validation`, naming the key. `?limit=5&limit=500` is not read as its
last value -- no route takes a list in its query, so a repeated key is
never meaningful. Build the query from a mapping, one value per key.

**Repeating a call.** A call that creates takes a required
`Idempotency-Key` header (1..200 characters):
`POST /api/v1/threads` and `POST /api/v1/threads/{id}/messages`. The
same key with the same request -- method, path with its query, body
bytes -- answers with the SAME thread or message in its current state,
and pings nobody again; the same key with another request is a 409
`conflict`. This holds when two requests with one key race as well: the
one that loses the insert answers as the creating request (`created:
true`, the same thread) or, with another body, the 409. Every other mutating call is safe to repeat by
construction, so it takes no key:

| call | why a repeat is safe |
|---|---|
| `PUT /recipients/{id}` | a versioned snapshot: the version is its key |
| `DELETE /recipients/{id}` | forgetting twice is forgetting once |
| `POST /threads/{id}/claim` | answers "is it yours now": `claimed: true` for the operator it belongs to -- even after the roster has dropped them since; a user thread is never claimed (403) --, 409 for anyone else |
| `POST /threads/{id}/assign` | answers "whose is it now": a thread already with that assignee is returned as it is |
| `POST /threads/{id}/release` | a thread already in the pool is returned as it is |
| `DELETE /threads/{id}` | a thread that is not there -- never was, or already deleted -- is answered as one deleted: 204 |
| `POST /threads/{id}/status` | sets a value; the same status is a no-op |
| `POST /threads/{id}/retag` | sets values; a repeat finds the thread already moved and back in the pool |
| `POST /threads/{id}/read` | the pointer only moves forward |
| `POST /sections` | create-or-find by key |
| `POST /inbox/{delivery_id}/read`, `POST /inbox/read-all` | set a value |
| `PATCH /preferences` | sets values |

`POST /threads/unread-counts` is a read that takes a body.

**Retag reads the subject by presence.** `POST /threads/{id}/retag`
takes `operator`, `section` and, optionally, the pair `subject_type` /
`subject_id`:

| the pair in the body | the thread's subject |
|---|---|
| both absent | kept -- the thread only moves to the section |
| both present, both `null` | cleared |
| both present, both set | set to them |
| one absent, or one `null` and one set | 422 `validation` -- half a reference names nothing |

In every form the thread returns to the section's pool (the assignee is
reset). A retag that would give the thread what another thread of the
same client in that section already has -- the same subject, or, for a
DM, the same absence of one -- is refused: threads are never merged.

**The recipient snapshot has a version.** Both write paths -- `PUT
/api/v1/recipients/{id}` and the `user_upserted` event -- carry
`version`, an integer the product increases with every change of the
person (a counter or an updated-at in milliseconds). comms applies a
snapshot only when its version is higher than the stored one: an event
that arrives late cannot roll the address book back. The stored version
with other content is a conflict. "No value" is an explicit `null` for
`telegram_id`, `email`, `locale` and `timezone` -- a blank string or a
telegram id of 0 is refused on both paths.

**Deleting a person.** `DELETE /api/v1/recipients/{id}` with
`{"version": N}`, or the `user_deleted` event `{v, recipient_id,
version}`. comms forgets every way to reach them -- chat, address,
language, time zone, schedule, groups, mutes, sections, read pointers --
and closes their waiting deliveries as `recipient_inactive`. The row
stays as a tombstone with the id alone, because the threads and messages
they were part of stay: a conversation belongs to both sides, and what
happens to message bodies -- and to their variants (section 12) -- is
the product's decision. That decision is a thread at a time: deleting a
thread (section 14) removes every message in it, whoever wrote it. A deletion is
final -- a person who returns gets a new id in the product. One copy is
not forgotten: the fields of the address book kept in a dead letter
(`<COMMS_EVENTS_STREAM>:dlq`, "How long comms keeps things") stay there
-- the products hide a person rather than erase them and comms follows
them, and that stream is for an investigation by hand, capped by
`DLQ_MAXLEN` and trimmed. A
recipient deactivated or deleted after a job was resolved is not sent
to either: their delivery ends `recipient_inactive`, which is neither a
failure nor a mute.

**A group is read when the job is delivered.** A notification to a
group reaches the members the group has when the job is resolved, at
delivery time -- not when it was sent. A `group_changed` that arrives
later is not seen by that job, in either direction: a member added
after resolve does not receive it, a member removed after resolve
still does. Send the membership before the job when the order matters.

### Channel health

`GET /health` says what a deploy has -- a channel's keys are set -- and
that is not the same as working: at one install it said `email: live`
while every letter died on a provider `401`. The second answer is what
each channel answered over a recent window:

    GET /api/v1/channels/health?window_minutes=60

`window_minutes` is 1..1440, default 60; outside that range it is
**refused** (422 `validation`), not clamped. The window is measured by
comms' database clock and closed at both ends. Behind the service token
like every `/api/v1` route; `GET /health` itself is unchanged.

```json
{
  "window": {"minutes": 60, "from": "2026-10-03T11:00:00+00:00", "to": "2026-10-03T12:00:00+00:00"},
  "channels": {
    "email": {
      "state": "live",
      "answers": 12,
      "by_outcome": {"accepted": 0, "refused": 12, "rate_limited": 0, "transient": 0, "timeout": 0, "error": 0},
      "refused_by_class": {"configuration": 12, "message_rejected": 0, "no_address": 0},
      "configuration_share": 1.0
    }
  }
}
```

Every channel of the service is listed (`in_app`, `telegram`, `email`,
`push`, `llm`), each with every key, zeros included -- an absent key never
means zero.

| field | meaning |
|---|---|
| `state` | the same channel map as `GET /health`: `live` / `not_configured` / `not_implemented` |
| `answers` | every answer the channel gave in the window -- one per call, so the retries of one letter count once each |
| `by_outcome` | the answers by what the channel said: took it, refused it, asked to wait (429), failed transiently, did not answer in time, or comms itself failed around the call |
| `refused_by_class` | the refusals by class: `configuration` -- the channel is dead on this deploy, fix the deploy; `message_rejected` -- this letter will not arrive, others go; `no_address` -- the recipient has no address in this channel, fix the sync |
| `configuration_share` | refusals of class `configuration` / `answers` -- the headline figure. **`null` when there were no answers**: nothing happened, which is neither healthy nor dead |

`live` with a `configuration_share` near 1 is the case this exists for:
the keys are there and the provider refuses them -- fix the deploy.
Suppressed recipients and scheduled waits are not channel answers and
are not counted. Answers are counted from comms' own transition record;
nothing is stored as "health", and comms acts on none of it.

### Lane depth

How many jobs wait in each lane right now -- the backlog of a lane no
worker takes (`COMMS_WORKER_LANES` without it) grows here and nowhere
else:

    GET /api/v1/lanes/depth

Behind the service token like every `/api/v1` route; read only, one
snapshot; `GET /health` is unchanged.

```json
{
  "lanes": {
    "interactive": {"ready": 0, "waiting": 0, "oldest_ready_seconds": null},
    "critical": {"ready": 0, "waiting": 0, "oldest_ready_seconds": null},
    "normal": {"ready": 3, "waiting": 12, "oldest_ready_seconds": 4.2},
    "bulk": {"ready": 250, "waiting": 0, "oldest_ready_seconds": 812.5}
  }
}
```

Every lane is listed, each with every key, zeros included -- an absent
key never means zero.

| field | meaning |
|---|---|
| `ready` | jobs the lane's next pass would take now -- the lane's own selection, its batch size aside |
| `waiting` | jobs still under way but not ready yet: scheduled for later, waiting out a retry, behind a pipeline failure's backoff |
| `oldest_ready_seconds` | how long the oldest ready job has been due; **`null` when none is ready** |

A job past its expiry that comms has not closed yet is in neither: it
will be closed, not delivered. The counts are read from the jobs at the
moment of the request; nothing is stored as "depth".

### Reconciling the address book

The book is synced eventually: an event that is lost or dead-lettered
leaves comms' copy behind the product's, silently. To compare, page
through what comms has recorded:

    GET /api/v1/recipients?limit=100&cursor=<next_cursor>

```json
{
  "items": [
    {"recipient_id": "8d0f6c1e-2b7a-4f0e-9a51-3c2d1e0f4b6a", "version": 1712345678901, "active": true, "deleted": false}
  ],
  "next_cursor": null
}
```

| field | meaning |
|---|---|
| `recipient_id` | the product's user id |
| `version` | the version of the snapshot comms holds; `0` -- written before snapshots had versions |
| `active` | as the snapshot set it; `false` for a deleted person |
| `deleted` | the person was forgotten: the row is a tombstone, nothing reaches them |

No address is listed, for anyone: comms refuses an equal version with
other content, so an equal version IS an equal snapshot -- compare
versions. Every recipient comms ever heard of is listed, tombstones
included. The order is oldest first, so a recipient created while you
page is reached on a later page -- unless its write was already under
way when you read the page before; a row that changes after its page
was read shows its old state. Either way the next pass sees it:
reconcile periodically, not once.

**The product decides; comms changes nothing on this read.** A version
behind yours: send the snapshot again (`PUT /recipients/{id}`). A person
missing: send them. A person you deleted who is not `deleted` here: send
the deletion. A tombstone you still have: that person cannot be revived
-- they need a new id. Group memberships and section rosters are not in
this listing.

### What comms takes on trust

comms checks ONE thing: the service token (`Authorization: Bearer`).
Every actor a request names -- `participant`, `operator`, `sender`,
`is_supervisor` -- is taken on trust. **Deriving them from the signed-in
user is the product proxy's job**: a proxy that forwards an actor from
its client lets a user read or write someone else's conversation, and
the defect is the proxy's. comms enforces what a role in a thread
allows (a participant posts in their thread, an operator in the threads
their section serves) -- never who the caller really is. This is the
one frozen part of the protocol; it is written down here, and
`app/api/deps.py` points at this paragraph.

## 6. Preferences

One object per recipient for a settings screen: the category toggles,
the delivery schedule and the recipient's time zone. Two routes,
`GET` and `PATCH` on `/api/v1/recipients/{recipient_id}/preferences`;
errors and trust are the protocol's (section 5). This section is the
whole contract -- `app/api/prefs.py` points here.

**The schedule is a list of ALLOWED periods.** A period says when comms
MAY deliver, not when it must stay quiet -- for every type except one
that declares `recipient_schedule: bypass` (section 3), which goes at
any hour. One period covers one day and
never crosses midnight: an evening allowance that runs into the night
is two periods, one per day. A screen built as "quiet hours" must
convert to this form before it writes -- sending its quiet window as-is
would store the opposite of what the person chose.

`GET` answers:

```json
{
  "categories": {"billing": true, "reminders": false},
  "schedule": [
    {"day": "mon", "from": "09:00", "to": "21:00"},
    {"day": "sat", "from": "10:00", "to": "24:00"}
  ],
  "timezone": "Europe/Berlin"
}
```

- `categories` -- one key per category the loaded profile declares,
  alphabetical; `true` = enabled (not muted).
- `schedule` -- the periods, or `null`: no schedule, deliver at any hour.
- `timezone` -- read-only, the product's own snapshot field; periods are
  read in this zone (the deploy default when `null`).
- `404` for a recipient comms has never been sent.

`PATCH` takes any of the two writable parts and answers with the full
`GET` form -- writing then reading is a fixed point:

```json
{
  "categories": {"reminders": true},
  "schedule": [{"day": "tue", "from": "08:30", "to": "12:00"}]
}
```

- `categories` -- PARTIAL: only the listed toggles change. An unknown
  category is a 422, and the whole PATCH is one transaction.
- `schedule` -- FULL REPLACE when present; `null` clears it; omitted,
  it is left untouched.
- An unknown key -- `timezone` included -- is a 422, never ignored.
- A PATCH that writes anything -- a toggle, a schedule, `null` -- for a
  recipient you deleted (`user_deleted`, `DELETE /recipients/{id}`) is a
  409 `recipient_deleted`, and nothing is written: a deleted person has
  no preferences. A deletion that lands while a PATCH is in flight
  waits for it and then forgets what it wrote.

A period:

| field | form | rule |
|---|---|---|
| `day` | `mon` `tue` `wed` `thu` `fri` `sat` `sun` | one day per period |
| `from` | `HH:MM`, `00:00`..`23:59` | the start |
| `to` | `HH:MM`, up to `24:00` | after `from` on the same day; `24:00` = end of the day |

`HH:MM` is exact: two digits `0`-`9`, a colon, two digits -- nothing
before or after it (`"09:00\n"`, `"09:00 "` are refused) and no other
script's digits; hour 24 only as `24:00`, only in `to`. Anything else
is a 422 `validation` naming `from` or `to`.

A list is refused (422) when it is empty -- send `null` to clear --
or when two periods of one day overlap or touch: one stretch is one
period.

## 7. The event protocol

Everything a product sends into the comms stream. This section is the
contract; the schema comments in `app/transport/events.py` follow it.

### The frame

One stream entry is `{event, data}` and, optionally, `request_id`:
`event` names the kind, `data` is a UTF-8 JSON document. The event
never changes; everything evolves inside `data`, which always carries
an integer `v` (supported: `1`).

`request_id` is the `X-Request-ID` of the request that wrote the event
(section 5, protocol 9.6), of the form `[A-Za-z0-9._-]{1,128}`. Every
log line comms writes while it processes the entry names it; one that
is absent or of another form -- not UTF-8 either -- is replaced by an id
comms mints, and the event is processed as it would be without the
field: an event is never refused, and never dead-lettered, because of
its `request_id`.
The field set of every event is **closed** -- `notification_request`,
`reminder_cancel`, `user_upserted`, `user_deleted`, `group_changed` and
`section_membership_changed`: an unknown field refuses the event, naming
the field -- a producer still sending a field comms no longer
takes learns it at once instead of believing it chose something.

Delivery is at-least-once. comms acknowledges an entry after it has
been processed, so any event may arrive more than once; the rules
below make every replay harmless.

### `notification_request` -- a job

A job is an **envelope** comms reads, plus a **letter** it carries and
never interprets.

| field | | envelope / letter | rule |
|---|---|---|---|
| `v` | required | frame | `1` |
| `idempotency_key` | required | envelope | string, 1..200 -- names the request (see below) |
| `type` | required | envelope | declared by the profile; the profile routes it to its channels |
| `target_type` | by type | envelope | `user` \| `group` \| `all` -- required for a type delivered to people, refused for an llm type (section 11) |
| `target_value` | by type | envelope | a recipient uuid, a group key, or `*`, matching `target_type`; with `target_type` or not at all |
| `scheduled_at` | optional | envelope | ISO 8601 with a zone -- "not before"; at most 3650 days ahead of the intake, refused under the key past that |
| `expiry_at` | optional | envelope | ISO 8601 with a zone; in the future and after `scheduled_at`; wins over the profile's `expires_after` |
| `correlation` | optional | envelope | string, 1..200 -- the product's own reference, stored untouched (see below) |
| `title` | by type | letter | 1..500 -- the stored fallback; templates override it at delivery. Required for a type delivered to people, refused for an llm type |
| `body` | by type | letter | 1..5000; as `title` |
| `action_data` | optional | letter | a JSON object: `action` (the deep-link intent), `params` (scalar deep-link parameters; one whose value is `null` is no parameter -- the telegram link is built as if it were absent), every other key a scalar template variable; at most 65536 bytes as compact UTF-8 JSON, the whole object; refused for an llm type |
| `llm` | by type | letter | the model's letter (section 11) -- required for an llm type, refused for any other |
| `email` | optional | letter | your own email, sent as it is (section 3a, "Your own email"): an object of `subject` (1..500, one line: no `\r` or `\n`), `text` (1..64000) and an optional `html` (1..256000, or absent, or `null`); none of them blank (only whitespace); no other key; `null` is no block. Only for a type with `email` among its channels -- refused for any other, an llm type included. One block per job: every recipient of the job gets the same email |
| `llm_audio` | optional | envelope | the signed link to the letter's audio body, and its format (section 11) -- an llm type only |

There is no channel and no priority on a request. The channel is the
profile's (section 3a). Which of the fields marked "by type" a request
carries is its type's: a type delivered to people takes the address and
`title` and `body`; an llm type (section 11) takes `llm` and none of
those -- either way the wrong set is rejected at intake, naming the
fields.

**`title` and `body` stay required with an `email` block.** They are the
letter of the type's other channels (the inbox, `in_app`); the email
does not use them.

**A string the database cannot store -- a NUL character (`\u0000`) or
a lone surrogate (`\ud800` and its kin, which have no UTF-8) -- in any
event is rejected at intake**, wherever it sits: a field, a value
inside `action_data`, `llm` or `email`, an item of a list, a key of an
object, a field's name. A `notification_request` is rejected under its
key, naming the field and never the value. In the `idempotency_key`
itself it makes the key unreadable: there is nothing to record the
rejection under, and the request goes to the dead-letter stream. Every
other event (`user_upserted`, `user_deleted`, `group_changed`,
`section_membership_changed`, `reminder_cancel`) goes to the
dead-letter stream at once, naming the field -- the event is malformed,
not failing.

**The same holds for every value of that class -- one comms cannot
store -- and for data nested too deep.** A number that is no JSON
number comms can store -- `NaN`, `Infinity`, `-Infinity` (some JSON
writers emit them), a literal past the float range (`1e309`), an
integer of more than 4300 digits -- is rejected naming the field; so
is `data` nested deeper than 64 levels (the `data` object itself the
first), and a `scheduled_at` or `expiry_at` that lies, in UTC, before
the year 1 or after 9999 (the year 1 at `+01:00`, say). A request
refused for what it holds is rejected under its key whenever the key
can be read. Data that cannot be read at all -- broken JSON, a nesting
past what the reader can follow -- leaves no key, and the request goes
to the dead-letter stream. So does a request whose key can be read but
whose handling failed -- transient failures past the retries while the
database answers, or an error no retry mends: this version of comms
gives it no outcome under its key, and the product sees that absence (a
read by the key finds no job).

**The size of an entry.** An entry whose `data` is larger than 4194304
bytes (4 MiB) is refused for its size before any of it is read, and goes
to the dead-letter stream with that reason, naming the field and the
limit; the dead letter keeps its size and fingerprint, not its data.
The number sits above the largest entry the field limits of this
section allow (an email job at its limits, about 1.3 MB in UTF-8).

**What intake answers.** A request is decided the moment it is read:

- **accepted** -- a new job under its key;
- **duplicate** -- the same key with the same bytes: the existing job, nothing new;
- **conflict** -- the same key with other bytes: recorded under the key, the existing job untouched;
- **rejected at intake** -- the key is readable but the request cannot be accepted (an undeclared type, a malformed address, an expiry already past, an unknown field): recorded under the key; the key is **not** taken, so a corrected request with the same key is accepted.

A request whose key cannot be read at all -- a field of the entry that
is not UTF-8 included -- goes to the dead-letter stream: there is
nothing to record the rejection under. The dead letter keeps its
address, never its letter, and the fingerprint of its `data` -- find
the request in your outbox by it (see "How long comms keeps things").

### The idempotency key names the request, not its subject

The key answers one question: **is this the same request again?** The
same key is the same job, for as long as comms keeps the job.

- A retry of the same request carries the same key -- that is what
  makes a replay harmless.
- A new intention is a new request and carries a new key, even when it
  concerns the same subject. Sending again after a cancellation, or
  scheduling again after a change of time, is a new request.
- A job that finished, expired or was cancelled **keeps its key**. The
  key is released only when the job itself is removed by retention
  (see "How long comms keeps things"). A key is never reused while its
  job exists: releasing it on cancellation would make "the same key"
  mean different things depending on the job's state.

How a product derives its keys from its own facts is the product's
decision. comms only compares them.

### `correlation` is opaque and compared by equality

`correlation` is how a product later finds its jobs again -- today, to
cancel them. comms stores it as given and matches it by **exact
equality** of the whole string. It never parses it, matches a prefix or
a part, or reads the letter to find a job. One job carries one
correlation; what it means is the product's.

### `reminder_cancel` -- cancel jobs by correlation

| field | | rule |
|---|---|---|
| `v` | required | `1` |
| `types` | required | a non-empty list of type keys |
| `correlation` | required | string, 1..200 -- equal to the `correlation` the jobs were sent with |
| `target_type` | optional | with `target_value`, both or neither; the same forms as in `notification_request` |
| `target_value` | optional | with `target_type`, both or neither |

Every job of one of `types` with that exact `correlation` (and that
target, when given) that has not reached an outcome is closed with the
outcome **cancelled**; its waiting deliveries are cancelled too, what
was already sent stays sent. A cancel that matches nothing is a
zero-row update, not an error, so a replay is harmless. A job being
delivered at that moment is cancelled after the attempt finishes.

### `user_upserted` -- the address book

comms keeps the product's recipients as **snapshots**. Every field is
required; "no value" is an explicit `null`, never an empty string and
never a telegram id of `0`.

| field | rule |
|---|---|
| `v` | `1` |
| `recipient_id` | the product's user uuid |
| `version` | integer `>= 1`, monotonic per user |
| `telegram_id` | integer, non-zero, or `null` |
| `email` | non-blank string or `null` |
| `locale` | non-blank string or `null` |
| `timezone` | IANA zone name or `null` |
| `active` | boolean |

### `user_deleted` -- forgetting

comms forgets how to reach the person and keeps only an id-bearing
tombstone.

| field | rule |
|---|---|
| `v` | `1` |
| `recipient_id` | the product's user uuid |
| `version` | integer `>= 1`, in the same order as `user_upserted` |

**The version orders the snapshots.** Older than the stored version --
refused as stale; equal with the same bytes -- a replay; equal with
other bytes -- a conflict; newer -- applied. The same rule orders a
deletion against snapshots, and a snapshot after a deletion is refused
at any version. The synchronous `PUT` / `DELETE
/api/v1/recipients/{id}` follow the same rule (section 5).

**Send a snapshot on every change**, not only on creation: a change of
language, zone or address, a deactivation, a block. comms re-checks
`active` right before a send -- against the snapshot it holds.

An event refused as stale is acknowledged and logged with its class;
it has no key to be recorded under.

**Forgetting does not reach a model's jobs.** A job of an llm type
(section 11) has no addressee -- its delivery's `recipient_id` is
`null` (section 8) -- so forgetting a person leaves it as it is, even
when its letter carries that person's words. Its letter, and the link
to its audio body, are deleted at the job's outcome; its answer lives
the type's `answer_keep` (see "How long comms keeps things"). A
person's words you may have to forget sooner than that belong in no
letter.

### `group_changed` -- membership

| field | rule |
|---|---|
| `v` | `1` |
| `group_key` | string, 1..200, opaque to comms |
| `recipient_id` | the product's user uuid; `user_upserted` comes first |
| `member` | `true` ensures the membership, `false` removes it |

Idempotent both ways. A group is read when the job is delivered
(section 5), so a membership that arrives later is not seen by a job
already delivered.

### `section_membership_changed` -- who serves a section

| field | rule |
|---|---|
| `v` | `1` |
| `section_key` | string, 1..100 -- the section, created if comms has not seen it |
| `section_label` | string, 1..200 -- used only when the section is created; an existing section keeps its label |
| `operator_id` | the operator's user uuid; `user_upserted` comes first |
| `member` | `true` adds the operator to the section, `false` removes them |

Idempotent both ways. The first `member: true` declares the section's
roster, for good: from then on only its members serve it, and once you
remove them all nobody does ("Serves the section", section 14). A
`member: false` for somebody who was never on the roster declares
nothing. The section may arrive before any thread is
opened in it: operators are hired before anybody writes in. An operator
comms has not been told about yet is retried, the same lag
`group_changed` has.

### Ordering the producer relies on

**comms' own database down does not lose your event.** When it does not
answer at all, comms waits -- without a limit, the delay growing to 30 s
-- and processes the event once it is back. When it does answer and one
event keeps meeting a transient failure, that event goes to the
dead-letter stream after a short bounded series of retries, so the
events behind it are not held.

- `user_upserted` for a new user precedes `group_changed` for them; a
  momentary inversion is retried by comms with a bounded backoff, a
  persistent one ends in the dead-letter stream.
- Sync events are safe to replay any number of times.
- `notification_request` replays collapse on the key.

## 8. Reading a job by its key

The product's last link when it asks "what became of it": its own
record -> its idempotency key -> the job in comms -> the steps.
Reading is the source of truth; nothing a later push says replaces it.

    GET /api/v1/notifications/by-key?key=<key>              the summary
    GET /api/v1/notifications/by-key/deliveries?key=<key>   a page
    GET /api/v1/notifications/by-key/path?key=<key>         a page

**The key is a query parameter, percent-encoded.** Any key the product
sent (1..200 characters, any characters) is read back by its exact
value. Encode it the standard way -- `/`, `?`, `%`, `#`, `&`, spaces and
non-ASCII all survive. One trap: a `+` sent **unencoded** reads as a
space; encode it as `%2B` (every standard query encoder does).

**Answers.** The same service token as every route (401 without it).
`key` empty, missing, longer than 200 or holding a NUL (`%00`, which
no accepted key holds -- intake refuses it) -> 422 `validation`. The
deliveries and path pages take `limit` and `cursor` like every listing
(section 5); a cursor from one listing is refused by the other (422).

- **404 `not_found`** -- the key answers nothing: comms never received
  it, **or** the job and its records outlived retention ("How long
  comms keeps things"). The two are not told apart. Before you look
  inside comms, look at your own outbox and relay: a 404 within the
  retention window means the request never arrived.
- **200, `job: null`** -- the request was **not accepted**: `intake`
  says why (rejected at intake, with the reason). The job was never
  taken; the responsibility is the product's.
- **200 with a `job`** -- accepted: comms owns it, and the job, its
  deliveries and its path say where it is.

The deliveries and path pages answer 404 when the key has no accepted
job (only intake outcomes, or nothing).

**What is never in an answer: the letter.** No title, body, action data,
template variables, no llm letter; no text of an exception of comms' own
(only its class and place, in `error`). A provider's own answer is in
the path, sanitized; once its recipient is forgotten it is `null`. The
one body a read carries is a MODEL'S answer to an llm job (`answer`,
below) -- the answer, never the letter it answered.

**A replay is not shown.** Sending the same bytes under the same key
again changes nothing and records nothing: the read shows the accepted
job, not a "duplicate". A conflict (the same key, other bytes) is
shown, in `intake`.

**The path starts with the journal.** Jobs accepted before the journal
existed (comms before migration 0017) have a partial or an empty
path; their summary and deliveries are whole.

**Adding a field is not a break.** Clients ignore fields they do not
know. Phase 5 added the model's answer to `job` that way -- the object
`answer` (the body, a snapshot of who answered, the letter's hash) --
without changing any field that was there.

### `summary` -- `GET .../by-key`

| field | value |
|---|---|
| `idempotency_key` | the key, as read |
| `intake` | every request under the key that was not accepted, oldest first (`intake_item`); `[]` when none |
| `job` | the accepted job (`job`), or `null` |

### `intake_item`

| field | value |
|---|---|
| `outcome` | `rejected_at_intake` -- the envelope could not be accepted; `conflict` -- the key is taken by an accepted job with other bytes |
| `reason` | for a human, redacted; the class is `outcome` |
| `received_at` | when it arrived |
| `notification_id` | the accepted job a conflict collided with; `null` for a rejection, and for a conflict whose job is past retention |

### `job`

| field | value |
|---|---|
| `id` | the job's id |
| `type` | the request type |
| `category` | the category it was gated by, or `null` |
| `target_type` | `user`, `group`, `all`; `null` for an llm job (it has no audience) |
| `target_value` | the address as sent; `null` for an llm job |
| `correlation` | as sent, untouched, or `null` |
| `status` | `pending`, `processing`; or the outcome: `sent`, `partial_sent`, `failed`, `no_recipients`, `suppressed`, `expired`, `cancelled` |
| `created_at` | when it was accepted |
| `scheduled_at` | when it becomes deliverable |
| `expiry_at` | when it expires, or `null` |
| `pipeline` | the pipeline's own failures (`pipeline`) |
| `deliveries` | the deliveries counted by channel and status (`delivery_count`); `[]` before resolve |
| `answer` | the model's answer of an llm job (`answer`); `null` for a job delivered to people |

### `answer`

The model's answer to an llm job (section 11). Read it on the push of the
job's outcome, within `keep_until`.

| field | value |
|---|---|
| `state` | `none` -- no answer: the job is active, or ended without one (its `status` says which); `kept` -- `body` is here; `purged` -- past `keep_until`, the body is gone, the rest stays |
| `letter_hash` | SHA-256 (hex) of the letter's canonical JSON (sorted keys, no spaces, UTF-8) -- which letter was asked; the letter itself is deleted at the outcome |
| `audio_hash` | SHA-256 (hex) of the audio body as fetched and sent (`llm_audio`), or `null` -- the body is part of the question; the link is not in the hash, a signature changes with every upload |
| `answered_by` | who answered (`answered_by`), or `null` when there is no answer |
| `answered_at` | when the answer came, or `null` |
| `keep_until` | until when the body is kept, or `null` |
| `body` | the provider's HTTP response body, as it came, as a string -- a chat completion the product parses itself; or `null` |

### `answered_by`

| field | value |
|---|---|
| `set` | the key set the call went through (`LLM_KEY_SETS`) |
| `provider` | the provider as the response names it (a gateway's `provider` field), or `null` when it does not |
| `model` | the model as the response names it, or `null` -- never the requested one |

### `pipeline`

| field | value |
|---|---|
| `attempts` | failed pipeline attempts **in a row**; 0 when none. An attempt that succeeds ends the run: `attempts` goes back to 0, `step` and `error` to `null` -- every failure stays in the job's path (`by-key/path`). The ceiling (`NOTIFICATION_MAX_PIPELINE_ATTEMPTS`) is of failures in a row: a long send in portions is not closed by a few failures spread over it. A transient failure of comms' own database (a deadlock, a lost connection, a restart) counts while that database answers; while it does not answer, nothing is counted or written |
| `step` | the step of the last failure of the current run: `lock`, `resolve`, `deliver`, `rollup`, `commit`; or `null` |
| `error` | the class and the place of the last failure of the current run, or `null` |
| `retry_at` | when the job is tried again after its last counted failure, or `null`; never with `attempts` 0 |

**A job STUCK at the ceiling.** A job whose `attempts` equal comms' ceiling
(`NOTIFICATION_MAX_PIPELINE_ATTEMPTS`) while its `status` is not an
outcome is stuck: comms failed at the ceiling to close it. That is a
defect of comms, not of your request -- its `error` names the class and
the place, and comms says it once in its log. No worker takes it again;
raising the ceiling in comms' settings makes it selectable once more, on
purpose.

### `delivery_count`

| field | value |
|---|---|
| `channel` | the channel |
| `status` | a delivery status (`delivery_item`) |
| `count` | how many |

### `delivery_item` -- `GET .../by-key/deliveries`

| field | value |
|---|---|
| `recipient_id` | the recipient, the product's id; `null` on the `llm` channel -- its delivery has no recipient |
| `channel` | the channel |
| `status` | `pending`, `sent`, `failed`, `suppressed`, `recipient_inactive`, `expired`, `cancelled` |
| `attempts` | channel attempts spent |
| `failure_class` | for a failed delivery: `configuration`, `message_rejected`, `no_address`, `transient_exhausted`, `pipeline`; else `null` |
| `wait_reason` | for a delivery that waits: `recipient_schedule`, `provider_rate_limit`, `transient_backoff`; else `null` |
| `next_retry_at` | until when it waits, or `null` |
| `sent_at` | when the channel took it, or `null` |
| `read_at` | when it was read (in-app), or `null` |
| `created_at` | when it was created (resolve) |

### `path_item` -- `GET .../by-key/path`

One row per transition, in order.

| field | value |
|---|---|
| `at` | when |
| `subject` | `job`, `delivery`, `channel` (what the channel answered), `gate` (a recipient a mute dropped before a delivery existed) |
| `step` | `intake`, `lock`, `resolve`, `deliver`, `rollup`, `commit`, `expire`, `cancel`, `withdraw`, `ceiling` |
| `outcome` | the status after it; for `channel`: `accepted`, `refused`, `rate_limited`, `transient`, `timeout`, `error`; for `gate`: `suppressed` |
| `recipient_id` | for `delivery`, `channel`, `gate`; else `null` |
| `channel` | for `delivery`, `channel`; else `null` |
| `attempt` | the attempt number: on a `delivery` or `channel` row, the delivery's; on a `job` row, the job's pipeline failures in a row at that row -- a failure's row carries its number in its run, which starts again at 1 after an attempt that succeeded |
| `wait_reason` | a wait: the delivery's, or `pipeline_retry` for the job; else `null` |
| `wait_until` | until when, or `null` |
| `failure_class` | as on a delivery, or `null` |
| `category` | the category a mute gate decided by, or `null` |
| `error` | an exception of comms' own: class and place, never its text; or `null` |
| `provider_text` | the provider's answer, sanitized; `null` when it said nothing or its recipient was forgotten |
| `template_unresolved` | on a `channel` row with `accepted`: the template's paths that did not resolve in the letter the channel took, as the template writes them (`["paid_amount", "user[name]"]`), their placeholders in the letter; `null` when every field resolved, and on every other row |

**A transient failure of comms' database.** While comms' database
answers, such a failure -- a deadlock that keeps coming back, say -- is
a failure like any other: an attempt is counted, its row in the path
carries the step, the class and the place and `wait_until`, and at the
ceiling the job gets its outcome -- the ceiling of failures in a row
(`attempts` above). While the database does not answer, nothing can be
written: the job keeps its state, no attempt is counted, and comms
takes it again once the database is back.

## 9. What changed since a cursor

A push from comms (section 10) is a hint; the read by key (section 8)
is the truth.
A product that was down -- longer than the stream keeps its pushes, or
at all -- asks which of its jobs changed while it was not listening, and
reads each of them by its key.

    GET /api/v1/notifications/changes?limit=20&cursor=<next_cursor>

```json
{
  "items": [
    {"idempotency_key": "order-42:paid"}
  ],
  "next_cursor": "<opaque>"
}
```

| field | value |
|---|---|
| `idempotency_key` | the key of a job that changed after the cursor -- the key the product sent; read the job by it |

That is all an item says: no status, no channel, never the letter. What
changed is in the read.

**The cursor.** Opaque; send back the `next_cursor` of the last page,
and keep it where your own state is kept. **It is never `null`**: a page
with nothing new still answers with a cursor to continue from, and
`items: []` means "nothing newer than this, for now". **Without a
cursor** the feed starts at the beginning of what comms keeps (section
"How long comms keeps things") -- the first call of a new product, and
the call of one that lost its cursor. Starting over repeats keys you
have seen; reading a key twice is harmless.

**Repeats are bounded, losses are not allowed.** A key appears once on
a page. It appears again on a later page only when its job changed
again -- or when one job wrote more than one page's worth of steps at
once (a broadcast): then its key heads the following page too. Repeating
the same cursor answers the same items (the `next_cursor` differs only
in when it was read).

**The feed is late, not lossy.** A change shows up only once comms is
sure nothing written before it can still appear: the feed waits for
every transaction that was running when it was read. The longest
transaction in comms is a delivery attempt calling a channel, up to that
call's timeout -- so the feed can lag by that long, and by any longer
transaction open on comms' database. Nothing is skipped; the next read
brings it.

**A cursor older than the retention period is refused**: 410
`cursor_expired`. What changed after it may already be deleted, and
continuing would hide that. A product that polls never meets it -- every
read, an empty one too, refreshes the cursor. One that was down for
longer: read each key it still holds as unfinished by key (section 8),
then start the feed again without a cursor. With retention switched off
(0) a cursor never expires. One residue is not caught: a job older than
the retention period that changed after your cursor may be deleted
before you read it -- its read by key answers 404.

**Answers.** The same service token as every route (401 without it).
`limit` outside 1..100 -> 422 `validation`; a malformed cursor, one from
another listing, or one comms could not have issued (from the future)
-> 422 `validation`. The feed reads only.

**Recovering after downtime.**

    1. Keep the last next_cursor with your own state, after you have
       acted on its page -- never before.
    2. Start: read the feed from the kept cursor (none -> from the start).
    3. For each item: read the job by its key (section 8) and update
       your record from the read.
    4. Store the page's next_cursor; repeat from 2 until a page comes
       back with items: [].
    5. Keep polling at your own pace; a push (section 10), when it
       comes, only says "go read" sooner.
    6. 410 cursor_expired: read every key you hold as unfinished by key,
       then start again at 2 without a cursor.

Jobs only: a request refused at intake is not a job and is not in the
feed -- the read by key of your own key shows it (section 8).

## 10. The push stream

A push says "this job changed -- go read it". It is a hint: the read by
key (section 8) is the truth, and a product that never listens loses
nothing but speed -- it reads by key and by the changes feed (section
9). comms never calls the product: it writes pushes into its own redis
(`COMMS_REDIS_URL`), and the product listens, the way comms listens to
the product's events.

**The stream.** `<COMMS_EVENTS_STREAM>:changes` -- with the default
event stream, `comms:events:changes`. comms creates it with its first
push. It is capped at `CHANGES_STREAM_MAXLEN` entries (default 100000,
approximate trimming): the oldest pushes are dropped, never the jobs.

**An entry.** Plain stream fields, nothing else:

| field | value |
|---|---|
| `v` | the push format version, `1`; a listener that meets another value stops and reconciles by the changes feed (section 9) |
| `idempotency_key` | the key of a job that changed -- the key the product sent; read the job by it (section 8) |

That is all a push says: no status, no channel, never the letter. What
changed is in the read.

**What pushes** is declared per type in the profile, `push_on`, and
taken at intake: a job keeps the value its type had when it was
accepted.

| `push_on` | a push comes when |
|---|---|
| `outcome` | the job reaches its outcome -- any status other than `pending` and `processing` |
| `outcome_and_deferral` | the job reaches its outcome, and each time the job or one of its deliveries is put behind a timed wait: the recipient's schedule, the provider's rate limit, comms' own backoff after a transient failure, the pipeline's retry gate |
| `none` | never -- the default, for a type that does not declare it |

Accepted, queued and in flight are not pushed: that is noise, and the
read shows them. A delivery that ends while the job goes on is not
pushed either; the job's outcome is.

**After the commit, at least once, in any order.** A push is published
only once the change it announces has committed, so the read always
finds it. A push may come twice, and two pushes of one job may come in
either order: both lead to the same read of the current job, so a
listener needs no ordering and no deduplication. A burst of changes of
one job may come as one push.

**A push can be lost; a result cannot.** Pushes written while the
product was not listening stay in the stream until it is trimmed. A
listener that starts -- the first time or after any stop -- first reads
the changes feed from its kept cursor (section 9), then follows the
stream; that is the whole recovery, and it covers a trimmed stream too.

**A listener**, whole -- `redis` is an asyncio client on
`COMMS_REDIS_URL`; `read_job` is your read by key (section 8) and your
own update from it; `reconcile` is the recovery of section 9:

```python
STREAM = f"{COMMS_EVENTS_STREAM}:changes"
GROUP, NAME = "product", "listener-1"   # your group; a stable name


async def listen(redis, read_job, reconcile):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    await reconcile()                   # what changed while you were away
    start = "0"                         # your own unacked entries first
    while True:
        reply = await redis.xreadgroup(
            GROUP, NAME, {STREAM: start}, count=100, block=5000,
        )
        entries = reply[0][1] if reply else []
        if start == "0" and not entries:
            start = ">"                 # then the new ones
        for entry_id, fields in entries:
            if fields[b"v"] != b"1":
                raise RuntimeError("unknown push format: reconcile")
            await read_job(fields[b"idempotency_key"].decode())
            await redis.xack(STREAM, GROUP, entry_id)
```

Read, then acknowledge -- never the reverse: an entry acknowledged
before its read is lost if the listener dies in between.

## 11. The llm channel

comms carries a letter to a MODEL and brings its answer back -- the
same delivery as any other, to a different kind of addressee. Writing
the letter and reading the answer are the product's: comms never reads
either. A translation, a summary, a transcript: different letters, one
channel.

### The key sets -- for the operator

The models a deploy can reach are **named key sets** in comms'
environment -- on a box asked of the operator by the passport under the
option `llm` (`primer.yaml`) -- one variable holding all of them as one
JSON object:

    LLM_KEY_SETS='{"llm.text": {"endpoint": "https://<gateway host>/api/v1", "key": "<key>", "model": "<vendor>/<model>", "gateway": true}, "llm.text_direct": {"endpoint": "https://<vendor host>/v1", "key": "<key>", "model": "<model>", "gateway": false}}'

It is read only with `LLM_ENABLED=true` (section 3a); with `false` the
deploy has no llm, the variable is not read, and the rest of comms works
as before. With `true` it is required, and every set must be whole, or
comms refuses to start, naming the set and the field -- never the
value. A set name is lower-case words joined
by dots (`llm.text`), at most 64 characters; a name written twice is a
refusal.

### `key_set` -- one set

| field | value |
|---|---|
| `endpoint` | the provider's OpenAI-compatible base address, `https://`, no credentials, query or fragment; comms appends `/chat/completions` |
| `gateway` | `true` -- the address is an aggregating gateway; `false` -- it is the vendor itself. No default |
| `key` | the provider's key; sent as `Authorization: Bearer`, never written anywhere else |
| `model` | the model id the request names; one ending in `:free` (a free route) refuses the start outside development |

**A gateway set carries the gateway's conditions in every request:**
`"provider": {"allow_fallbacks": false, "data_collection": "deny"}` --
the gateway does not silently fall back to another provider, and only
providers that do not collect the data are used. A vendor set
(`gateway: false`) never carries the block: a vendor refuses a
parameter it does not know. Zero data retention is not asked for: check
the gateway's terms for the chosen model before production.

**The cost is the key's.** comms counts no tokens and keeps no budget:
put a credit limit on the key at the provider. A key out of credit is a
`configuration` failure -- the set is dead, fix the deploy.

**A call to one set may take `LLM_DELIVER_TIMEOUT_SECONDS`** (default
120; above the channel's client, 114 s, or the start is refused). A
job's call over its list may take that many times the number of its
sets, and 35 s more when it fetches an audio body first. It is the channel's own bound, not a lane's: the lanes' 30 s stay
what they are for email and telegram.

### The type -- for the product's profile

A type whose `channels` are exactly `[llm]` asks a model. `llm` goes
alone: with another channel in the same type the start is refused.
Its own fields -- `llm_sets` (required), `answer_keep` and
`max_audio_bytes` -- are in the type record table (section 3), with
their values and what they default to; they are refused on any other
type. `category`, `recipient_schedule` and `max_body_chars` concern a
person and are refused on an llm type. `lane`, `push_on`,
`expires_after`, `retry_max_attempts` and `retry_backoff_seconds` work
as for any type.

    support_reply_draft:
      channels: [llm]
      llm_sets: [llm.text, llm.text_direct]
      lane: interactive
      push_on: outcome
      answer_keep: 24h
      expires_after: 10m

**Where an llm type belongs: a lane that, on this box, carries no
types for people and has more than one job in work at once** (the table
under `lane`, section 3). A model writes for up to the bound above and
holds one of its lane's places all that time: as many model calls as
the lane has places hold the whole lane, and every job of a type for
people in it waits behind them. In `bulk`, with one place, llm jobs go
strictly one after another: a recording cut into ten pieces is ten
calls end to end. A profile that mixes the two kinds in one lane
starts, and the worker says so in one warning line, `lane_mixes_llm`,
naming the lane, its jobs in work at once (`jobs`) and both sets of
types.

### `llm` -- the letter

The request (section 7) carries no address, no `title`, `body` or
`action_data` -- only `llm`:

| field | value |
|---|---|
| `messages` | required: the chat messages, a non-empty list of objects, each with a non-empty `role` and a `content` (a string or a list of content parts); handed to the model as they came |
| `response_format` | optional: an object handed to the provider's structured output as it came (a JSON schema the answer is held to) |
| `params` | optional: the model's parameters (`temperature`, `max_tokens`, ...), handed over as they came |

`params` may not name what the transport writes: `messages`, `model`,
`models`, `provider`, `response_format`, `route`, `stream`,
`stream_options` -- the model is the key set's, the routing is the
gateway's, the schema is `response_format`; such a request is rejected
at intake. The letter is at most 200 000 characters of canonical JSON.
comms checks the letter's FORM only; what it says is never read, logged
or kept past the job's outcome -- its hash is. The job has no addressee,
so forgetting a person does not reach it (section 7, `user_deleted`).

### `llm_audio` -- an audio body by link

A recording does not travel in the envelope: megabytes of a person's
voice would sit in your outbox and in the stream. Keep
the body in your own object store and put a SIGNED link to it in the
envelope, beside the letter (never inside it -- comms does not read the
letter):

| field | value |
|---|---|
| `url` | a signed GET link to the object on the INTERNAL address of your store (path-style), `http://` or `https://`, no credentials or fragment, at most 4096 characters; comms fetches it as it is -- the same host and port, no proxy, no redirect followed |
| `format` | the audio format the model is told, 1..16 lower-case letters or digits (`wav`, `mp3`, ...) |

**Where the body goes.** comms fetches the body and appends ONE message
to the end of the letter's `messages`:
`{"role": "user", "content": [{"type": "input_audio", "input_audio": {"data": "<base64>", "format": "<format>"}}]}`.
Nothing in your messages is read or changed: write the prompt knowing
the audio comes last. The link never goes to the model -- the bytes do.

**The operator names the store.** `LLM_BODY_HOSTS` in comms' environment
is the exact `host:port` pairs a link may point at, comma separated
(`rustfs:9000`). Empty, the default: this deploy fetches nothing, and a
request with `llm_audio` is rejected at intake. Both only with the llm
channel on: with `LLM_ENABLED=false` the list is not read, and the job
is accepted and ends in the outcome "channel unavailable" (section 3a). A link to any other
host -- `comms-postgres`, `comms-redis`, `comms-app`, a metadata
address -- is rejected at intake, and refused again at the attempt if
the list changed since.

**Every attempt fetches the body again,** and comms keeps it only in the
attempt's memory -- never in its database, stream or dead letters. So:

- **the link and the object must live at least as long as the job**
  (its `expiry_at`, or the type's `expires_after`, with its retries);
- the link is kept on the job until its outcome and deleted then, with
  the letter; it is in no answer, path, log line, error text or dead
  letter;
- deleting the recording after the outcome is yours.

| the store answers | the delivery |
|---|---|
| 200, within `max_audio_bytes` | the body is sent to the model (every set the call reaches) |
| 200, larger than `max_audio_bytes` (by its `Content-Length`, or counted while reading) | `failed`, `message_rejected` -- read no further, no model asked |
| 403 (an expired signature included), 404, any other 4xx, a redirect | `failed`, `message_rejected` -- no model asked |
| 408, 429, 5xx, no connection, no answer in 35 s | transient: retried by the type's rule, the body fetched again |

**A resend** is the same bytes under the same key -- the same link
included: nothing new is done. Another link under the same key is a
`conflict`, as for any field. Asking again with a fresh link is a new
key.

### What comes back

**Any answer the model gives is a delivered answer.** A refusal in
words, a filtered or a cut answer, an answer that does not fit the
schema asked for: the delivery is `sent`, the job's outcome `sent`, and
the body is in the read. Whether it is good is the product's to judge --
and to ask again with a new letter, under a new key.

**A failure is the transport's only,** and only then is the NEXT set of
the list asked, in the same call: the status of the response (401, 402,
403, 404 -- the set is wrong, except a 403 that declines the request on
a policy -- a moderation flag, a guardrail, a content filter, read from
the provider's `error.metadata` -- which is this request's; 429; 408,
409, 5xx), a failure the
provider puts inside a 200 (an `error` object), an answer that is not a
chat completion, an answer larger than 4194304 bytes (by its
`Content-Length`, or counted while reading -- read no further), no
answer in time. A request the provider refuses
(another 4xx) goes to the next set too. When every set failed:

| the sets | the delivery |
|---|---|
| every one asked to wait (429 with `Retry-After`) | waits the shortest of the waits, like any channel's rate limit |
| any failure a retry can fix | an attempt is spent; it is retried from the first set, by the type's retry rule; `transient_exhausted` at the end |
| none of those, and one refused the request | `failed`, `message_rejected` |
| every one refused on configuration | `failed`, `configuration` |

The path (section 8) shows what each set answered, by status and the
provider's error code -- never the provider's error message, which may
quote the request.

**The answer.** On the job's outcome (`push_on: outcome` pushes it),
read the job by its key: `answer.body` is the provider's response body,
`answer.answered_by` the set, provider and model that answered. The
body is kept for the type's `answer_keep` after it came -- read it again
as often as needed within it -- and then deleted (`state: purged`); who
answered and the letter's hash stay with the job, which general
retention deletes like any other.

## 12. A message in another language -- variants

Translation in a thread is **your** use of the llm channel (section
11): you decide who reads which language, you ask a model with your
prompt and your schema, you read its answer. **comms does not
translate, does not decide who needs a translation, and reads neither
the message nor the model's answer.** What it gives is a place for the
result -- a **variant** next to the message -- and the thread feed in
the reader's language, which says for every message the variant or why
there is none.

**The original is never changed.** A variant is a separate record; no
call edits or replaces the author's text. One mechanism for both
directions -- the client writing to an operator and back: what differs
is your type (its sets, its lane), not the record.

**A language** is an opaque token: 1..8 characters of `[A-Za-z0-9_-]`,
compared byte for byte -- `pt-BR` and `pt_br` are two languages to
comms. It never comes from the person's settings (`recipients.locale`
says what they want to read, not what they wrote): the language a
message is written in is the one **the model found**.

### Two records, both yours, both insert-only

Both take the `Idempotency-Key` header (section 5): the same key with
the same request answers with the same record; the same key with
another request is 409 `conflict`. Neither pings anyone. A message that
does not exist, or is not in that thread, is 404.

**`POST /api/v1/threads/{tid}/messages/{mid}/variant-requests`** --
write it when you queue the model: "a variant in this language was
asked for, by this job". It is how the feed can say `preparing`: the
link between your job and the message is yours, and comms is told it,
never guesses it from your keys.

| field | in the body | value |
|---|---|---|
| `locale` | required | the language asked for |
| `job_key` | required | the `Idempotency-Key` of your llm job (section 11); a key that names no llm job is 422 |

The answer carries `message_id`, `locale`, `job_key`, `created_at`.
Several requests for one language are fine -- after a failed job,
queue another and request again; the feed reads the latest. The same
job for the same language twice under another key is 409 `conflict`.

**A request is tied to the job its key named when it was written.**
Retention deletes a job (`NOTIFICATION_RETENTION_DAYS`) and frees its
key: a later job under the same key is another job, and it does **not**
change the request -- whose own job is gone, so it reads `impossible`.
Want the new job to count? Write a new request for it.

**`POST /api/v1/threads/{tid}/messages/{mid}/variants`** -- write it
when you have read the model's answer. One per language, **never
replaced**: a second variant in a language is 409 `conflict`.

| field | in the body | value |
|---|---|---|
| `locale` | required | the variant's language |
| `job_key` | required | the llm job that answered; one without an answer is 422 |
| `source_locale` | see the forms | the language the model found the message written in |
| `body` | see the forms | the text in `locale`: 1..5000 characters, not blank |
| `declined` | optional, `false` | `true`: you read the answer and write no variant |

Exactly one of three forms -- anything else is 422, naming what is
wrong:

- **the text**: `body`, and `source_locale` other than `locale`;
- **none is needed**: `source_locale` equal to `locale`, no `body` --
  the message is already in that language;
- **declined**: `declined: true`, no `body`; `source_locale` may be
  given, never equal to `locale`. The model answered and could not --
  in its words, which comms does not read: only you can say it.

The answer carries `message_id`, `locale`, `source_locale`, `body`,
`declined`, `job_key`, `answered_provider`, `answered_model`,
`created_at`. **`answered_provider` / `answered_model` are not yours to
send**: comms copies them from the job's answer (section 11) -- the
provider and model that answered -- and keeps them with the variant
after the job is gone.

### The feed in the reader's language

`GET /api/v1/threads/{tid}/messages?locale=<language>` pages exactly as
without it. Every item carries the message as always, and two more
keys:

| key | value |
|---|---|
| `variant` | when `variant_state` is `ready`: `locale`, `source_locale`, `body`, `job_key`, `answered_provider`, `answered_model`, `created_at`; otherwise `null` |
| `variant_state` | one of the five below, never empty |

The state, decided in this order -- the first that holds:

| | what is recorded for the message | `variant_state` |
|---|---|---|
| 1 | the variant in the reader's language, with text | `ready` |
| 2 | the variant in the reader's language, declined | `impossible` |
| 3 | the variant in the reader's language, none needed | `not_needed` |
| 4 | another variant whose `source_locale` is the reader's language | `not_needed` |
| 5 | the latest request in the reader's language, its job queued, in work or answered | `preparing` |
| 6 | the latest request in the reader's language, its job failed, expired or cancelled -- or deleted by retention, whatever its key names now | `impossible` |
| 7 | none of these | `not_requested` |

**`impossible` has two sources**, and both mean "there will be no
variant unless you ask again": the transport's -- the job ended without
an answer (row 6) -- and the content's -- you read the answer and
declined (row 2). A job that **answered** is `preparing` until you
write the variant: an answer saying "I can't" is still an answer
(comms does not read it), and `declined` is how you say so.

**`not_requested` is where you decide**: nothing was asked for that
language and nothing says the message is in it -- queue the model if
the reader needs it. The feed never queues anything by itself.

Without `locale` the feed is **exactly** what it was: no `variant`, no
`variant_state`.

### How long a variant lives

As long as its message: a message's variants and requests go with it.
A message goes only with its thread, when you delete the thread
(section 14): retention removes notifications, closing a thread is a
status change. **Forgetting a person keeps the
variants, as it keeps the bodies of their messages** (section 5) --
what happens to a forgotten person's words in a shared conversation is
your decision, for the variants as for the bodies.

### End to end

A client writes in German to a support operator who reads English.

1. The message is posted as always: `POST /threads/T/messages`, its id
   `M`.
2. You decide the operator needs English and queue your llm type
   (section 11) with your prompt, the text of `M` you read, and your
   schema -- `Idempotency-Key: translate:M:en`, a key of yours.
3. You record it:

       POST /api/v1/threads/T/messages/M/variant-requests
       Idempotency-Key: request:M:en
       {"locale": "en", "job_key": "translate:M:en"}

   The operator's feed with `?locale=en` says `preparing` for `M`.
4. The job's outcome is pushed (section 10); you read the job by its
   key (section 8), parse the answer with your schema: the text, and
   the language the model found -- `de`.
5. You write the variant:

       POST /api/v1/threads/T/messages/M/variants
       Idempotency-Key: variant:M:en
       {"locale": "en", "job_key": "translate:M:en",
        "source_locale": "de", "body": "I can't log in since yesterday"}

6. The feed with `?locale=en` gives `M` as written, its `variant` in
   English with the provider and model that answered, and
   `variant_state: ready`. With `?locale=de` -- the client's own feed --
   `M` is `not_needed`: the model found it written in German.

## 13. The thread list -- for the operator and for the client

`GET /api/v1/threads` lists **one reader's** threads, most recently
active first: an **operator's** -- the threads they may see -- or a
**client's** -- the threads in which they are the client, their
conversations with the other side and with support in one list.
Exactly one reader per call.

**Trust (spec §10.11): `operator`, `client` and `is_supervisor` are
taken on faith.** comms checks the service token, not who is asking.
Your proxy derives the reader from its session and never forwards one
the user sent: a client the user chose is the user reading someone
else's conversations, and the fault is the proxy's.

| parameter | value |
|---|---|
| `operator` | uuid -- the operator whose visible threads are listed: assigned to them, or unassigned in a section they serve |
| `client` | uuid -- the person whose threads as the client are listed; never one where they are the operator |
| `is_supervisor` | `true` widens an operator's read to every thread; with `client` it is 422 |
| `with_unread` | `true`: each row the reader takes part in carries `unread` -- their unread messages in it |
| `with_last_message` | `true`: each row carries `last_message` |
| `locale` | the reader's language (section 12's form); only with `with_last_message`, else 422 |
| `limit` | page size, 1..100 (section 5) |
| `cursor` | the previous page's `next_cursor` (section 5) |

Both readers, or none, is 422 `validation`; so is a parameter given
twice. The order is `COALESCE(last_message_at, created_at)`
descending, then `id` descending -- a message moves its thread up --
and the cursor walks it the same way for both readers.

**A row** is the thread: `id`, `client`, `operator_kind`,
`operator_value`, `assignee`, `kind`, `status`, `subject_type`,
`subject_id`, `title`, `priority`, `last_message_at`, `created_at`,
`revision` (section 14, "The thread's revision").
Opt-in keys are added to it, never anything else:

| key | when | value |
|---|---|---|
| `unread` | `with_unread=true`, on a row the reader takes part in | an integer: messages not sent by the reader, newer than their read pointer (`POST /threads/{id}/read`). On the client's list every row has it; on an operator's, an unclaimed section thread in their pool has none -- not theirs, which a zero would hide |
| `last_message` | `with_last_message=true`, on every row | `id`, `sender`, `body`, `created_at`; with `locale` also `variant` and `variant_state`; `null` when the thread has no message |

**Without these parameters every row is exactly what it was.**

`last_message` is the message the thread's feed gives first: the
newest by `created_at`, and of two of the same instant the one with
the greater `id`. It is what is stored -- comms translates nothing and
keeps no copy for you.

### The preview in the reader's language

With `locale`, `last_message` carries `variant` and `variant_state`
exactly as an item of the feed in that language does (section 12) --
the same decision, made by the same code. A person who must never see
an answer in a language they do not read should be shown by this:

| `variant_state` | show in the row |
|---|---|
| `ready` | `variant.body` -- the message in the reader's language |
| `not_needed` | `body` -- the message is already in the reader's language |
| `preparing` | not `body`: a translation is on its way |
| `impossible` | not `body` silently: there will be no translation -- your decision what to say |
| `not_requested` | not `body`: **nothing has been translated yet -- do not show the original as if it were the reader's language**; decide whether to ask a model (section 12) |

## 14. Thread verbs -- claim, assign, release, status, retag

The operator side of a conversation. Every verb answers the thread in
its current state; section 5 says why each one is safe to repeat.

**Trust (spec §10.11): `operator` is the actor and is taken on faith.**
comms checks the service token, not who is asking. Your proxy puts the
operator from its own session and never forwards one the user sent.

| call | body | who may call | answer |
|---|---|---|---|
| `POST /threads/{id}/claim` | `operator` | an operator who serves the thread's section | `{claimed, thread}`; 409 `conflict` when another operator holds it |
| `POST /threads/{id}/assign` | `operator`, `assignee` | the thread's assignee, or an operator who serves its section | `<thread>`, its `assignee` the one named |
| `POST /threads/{id}/release` | `operator` | the thread's assignee, or an operator who serves its section | `<thread>`, its `assignee` `null` |
| `POST /threads/{id}/status` | `operator`, `status` | the thread's assignee; on a section thread also an operator who serves its section | `<thread>` |
| `POST /threads/{id}/retag` | `operator`, `section`, `subject_type`, `subject_id` | as for `status` | `<thread>`, back in the new section's pool (section 5) |

"Serves the section": a section whose roster was declared
(`section_membership_changed` with `member: true`, section 7) is served
by its members, and a section whose operators were all removed is served
by nobody -- its pool shows to nobody, nobody may claim it, and a new
message in it pings nobody; a section whose roster was never declared
is served by every operator, as before any roster existed. The thread's
assignee may act on it even after leaving the roster -- whoever took a
conversation can finish it or hand it on. A section emptied before comms
3.14.0 has no record of its roster and is served by every operator until
you declare a member of it again.

The right is decided when the call writes: an operator removed from the
roster while their call is in flight either acted before the removal or
gets 403 -- the removal never lands between the check and the write.

### Handing a ticket over -- `assign` and `release`

Only a **section** thread changes hands. A DM's operator is its master,
fixed by the form: `assign` and `release` refuse it.

`assign` hands the thread to `assignee` -- over whoever holds it, or out
of the pool. `assignee` must serve the thread's section. Once it is
done the new assignee writes into the thread and the previous one no
longer can (`POST /threads/{id}/messages` answers them 403). `release`
returns the thread to its section's pool: no assignee, claimable again.

**A release calls nobody.** No notification goes to the section's
roster, and none to anybody on an `assign`. The released thread is in
the pool's list again (`GET /threads?operator=`, section 13); the next
message in it pings the roster as for any thread in the pool. If your
operators must be told now, tell them yourself.

**Two calls that cross.** comms pins each write to the thread as the
call found it -- its assignee and its section. A claim, an assign, a
release or a retag that landed in between makes the later call lose
rather than write over it: 409 `conflict` -- read the thread and decide
again. Of two crossing calls exactly one changes the thread. The same
call twice is not a crossing: the second finds its own outcome and
answers 200.

| status | class | when |
|---|---|---|
| 404 | `not_found` | no such thread; no such `assignee` recipient |
| 403 | `forbidden` | `operator` is neither the thread's assignee nor serves its section |
| 422 | `validation` | the thread is a DM; `assignee` does not serve the section; a body field missing, `null` or unknown |
| 409 | `recipient_deleted` | `assignee` was deleted |
| 409 | `conflict` | the thread changed between your read and the write |

### Status

Three statuses: `open`, `resolved`, `closed`. A manual move, by row
the current status, by column the one asked for:

| from \ to | `open` | `resolved` | `closed` |
|---|---|---|---|
| `open` | no-op | yes | yes |
| `resolved` | yes | no-op | yes |
| `closed` | yes | 422 | no-op |

The same matrix for a DM and for a section thread. "no-op" answers 200
with the thread unchanged; 422 is `validation`. Besides the manual
moves, a message from the client returns a `resolved` or `closed`
thread to `open`, as it always did; a message from the operator side
moves nothing. Two status calls that cross queue on the thread: the
second is decided on the status the first left.

**Closing a section thread tells the client** (`msg.thread_closed`):
comms' maintenance pass issues the notice shortly after the close.
**Returning the thread to `open` -- by hand or by the client's message
-- before the notice is issued cancels it:** comms never issues it for
a thread that is open again. A notice issued before the reopen is
delivered like any other. A DM is never announced.

**Returning a thread to `open` does not restart its idle clock.**
Auto-close (`THREAD_AUTO_CLOSE_DAYS`) measures silence from the last
message, and a manual reopen writes no message. With auto-close on, a
thread that has been silent longer than that and is reopened by hand is
closed again by the next auto-close pass -- a section thread with a new
notice to its client. Keep `THREAD_AUTO_CLOSE_DAYS=0` if your operators
reopen idle conversations, or write into the thread when reopening it.

### The thread's revision

Every answer that carries a thread -- `POST /threads`, every row of
`GET /threads`, `claim` (in its `thread`), `assign`, `release`,
`status`, `retag` -- carries `revision`, an integer: the thread's state
as of that answer, after the call's own write.

**A thread starts at 1, and every write of it raises the number by
one** -- whoever writes: your calls (`claim`, `assign`, `release`,
`status`, `retag`), a message in the thread (it moves the thread's
activity; a client's message also reopens it), and comms itself
(auto-close; the bookkeeping of the close notice). So the number can
grow while nothing you read changed, and it grows on writes that do not
touch the assignee. A call that writes nothing -- a repeat that finds
its outcome already there, a claim of a thread somebody holds, a
status asked for that the thread already has -- leaves it as it was.

**Keep your copy of a thread only if the answer's `revision` is greater
than the one you keep.** Two answers about one thread can reach you in
the other order -- two hand-overs, a hand-over and a status -- and the
greater number is the later state. Compare numbers of ONE thread only:
two threads' numbers mean nothing to each other, and a deleted thread
ends its sequence (a new thread of the same subject starts at 1 under a
new id).

### Deleting a thread

`DELETE /api/v1/threads/{id}` -- no body, no actor -- answers **204**,
and nothing else for a thread that is not there: never created, or
deleted already. A repeat after a lost response ends where the first
call did. Who may delete, and why (spam, a retention rule of yours), is
your decision: comms takes the service token and asks nothing more.

**What goes**, in one transaction: the thread, every message in it --
whoever wrote it -- the variants and variant requests of those
messages (section 12), and the participants' read pointers in it.

**What stays**: the people (their recipients); the notifications the
thread has already given birth to; a model's job asked for one of its
messages, and the job's answer, until notification retention (above).

**After it, the thread is a thread that never existed.** Every route
that names it answers 404 `not_found` -- the messages and the feed,
`unread-count`, `read`, the variant routes, `claim`, `assign`,
`release`, `status`, `retag`. The thread lists, `POST
/threads/unread-counts` and the unread summary do not contain it.

**The feed of a thread that does not exist is 404, not an empty page;
its `unread-count` is 404, not 0.** This holds for a deleted thread and
for an id that never was one alike. A malformed `cursor`, `limit` or
`locale` is still 422 first.

**A message ping or a close notice already issued is delivered.** It
does not carry the conversation's text, and its link
(`open_thread__<thread id>`) then leads your app to a thread that
answers 404 -- show the person that the conversation is gone. A close
notice not issued yet when the thread is deleted is never issued.

**A new thread of the same subject is a new thread**, with a new id: a
DM of the same pair, a ticket about the same entity. The same goes for a
repeat of the old create request: its `Idempotency-Key` went with the
thread, so `POST /threads` with it creates a new one -- as a job's key
is free again after notification retention.

**A model's job for a message of the deleted thread** runs to its
outcome as any job; writing the variant from its answer is a 404.

**Calls that cross a deletion.** A write on the thread that starts
while it is being deleted either finishes first -- and what it wrote
goes with the thread -- or waits for the deletion and answers 404. A
read does not wait: it answers the thread as it was, or 404 -- and a
feed page read in the very instant the deletion lands can come back
empty. None of them fails otherwise.

**Forgetting a person and deleting a thread, side by side.** Forgetting
(section 5) keeps the threads and the bodies of the forgotten person's
messages -- a conversation belongs to both sides. Deleting a thread
removes the messages of both sides in it. The two do not conflict: the
first is about a person, the second about a conversation.

## 15. Under Primer's machine -- `primer.yaml` and the seams

Under Primer's deploy machine comms is a service: a repository, the
file `primer.yaml` at its root, and a tag. The file holds no value --
only where each value of the instance comes from: minted by the machine
(the database and Redis passwords, the service token), asked of the
operator (the channels' secrets), a parameter of the instance (the mail
domain, the sender, the region, the host of Telegram links), the
passport (the channel flags, `options.<channel>`), the machine's
addresses, or derived by comms itself. A key of a switched-off channel
holds the machine's literal, which comms never reads (section 3a). The
file is the list; this section does not repeat it.

**The seams: what comms gives the product.** Each is a key of comms'
`env`, and the product takes it by the same name as a seam from comms:

| seam | what it is |
|---|---|
| `COMMS_API_URL` | the address of `comms-app` on the machine's network -- the comms HTTP API |
| `COMMS_SERVICE_TOKEN` | the shared secret the product presents as `Authorization: Bearer <token>`; minted |
| `COMMS_EVENTS_ADDRESS` | `<host>:<port>` of comms' Redis, the point `events` -- where the relay writes `notification_request` and the push stream is read (sections 7, 10); no scheme |
| `COMMS_EVENTS_PASSWORD` | the password of that Redis; minted |
| `TELEGRAM_BOT_URL` | `https://<host>/<bot username>`, derived by `comms derive bot-url` (below); only with the `telegram` option |

`TELEGRAM_BOT_ID` waits for the machine: it refuses a derived value
shorter than 16 characters, and a bot id is 9-10 digits.

**`comms derive`.** The machine runs `comms derive <name>` in a
throwaway container of the comms image, with the instance's other
values in its environment; the value is the command's one line of
output.

- `bot-id` -- the digits before the colon of `TELEGRAM_BOT_TOKEN`, the
  bot's id. No network.
- `bot-url` -- `https://<TELEGRAM_LINK_HOST>/<username>`, the username
  asked of Telegram (getMe, bounded by the telegram channel's client
  timeout). The host is a parameter of the instance, never a literal of
  code: the link host has changed before.

A wrong token or host is a non-zero exit and one line on stderr naming
the variable -- never the token.

## How long comms keeps things

The questions every product asks once, answered here so the answer is
not a trip through our source. The history and the threads are
deploy-side settings with defaults; both are days-granular, and both
treat zero as "never" (a negative value refuses the start, naming the
variable). The dead letters are capped by count, a deploy-side setting
too. A model's answer is kept by its type.

- **Notification history**: `NOTIFICATION_RETENTION_DAYS`, default
  **90**. A notification in a terminal state (and its deliveries) is
  removed by the worker's retention pass after that many days, counted
  from the later of its creation and its `scheduled_at` -- a job sent
  late lives its days from when it was due -- except a `cancelled` one,
  counted from its creation: a cancel is the one outcome a job can
  reach before it is due, and a reminder cancelled years ahead is not
  kept for those years. Nothing is removed while a delivery is still
  pending, nor while a push of the job (section 10) is still owed --
  it goes once comms has published it.
- **A model's answer** (section 11): the type's `answer_keep`, default
  **24h**, counted from the answer; the letter it answered, and the
  link to its audio body, are deleted at the job's outcome; the body
  itself is never kept. None outlives the notification history.
- **A thread**: until you delete it -- `DELETE /threads/{id}`
  (section 14) removes it with its messages, their variants and
  variant requests, and the read pointers. comms deletes no thread by
  itself, on no schedule.
- **Threads closing themselves**: `THREAD_AUTO_CLOSE_DAYS`, default
  **30**. A thread with no activity for that long is closed by the
  same worker. Closing is a status change -- the thread, its messages
  and the participants' read pointers stay.
- **A message's variants** (section 12): as long as the message, which
  goes with its thread when you delete the thread -- and forgetting a
  person keeps them, as it keeps the bodies of their messages.

- **Dead letters**: an entry comms cannot take -- a broken frame, an
  unknown event, a request whose key cannot be read, an event that
  failed past its retries -- lands in the dead-letter stream
  `<COMMS_EVENTS_STREAM>:dlq` in comms' redis, for the investigation.
  It keeps the entry's address and never its letter (the table below).
  No expiry: the stream is capped at `DLQ_MAXLEN` entries, default
  **10000**, the oldest trimmed first (approximately). Nothing is
  replayed from it: once the cause is fixed, your outbox sends the
  event again.

A deploy that wants either behaviour off sets the value to 0. A
product that needs history kept longer than comms keeps it should
retain its own copy: comms is the delivery service, not the archive.

**What a dead letter holds.** A value is kept only where the table
says so; every other field of the entry, and every other key of its
`data`, is dropped and only its NAME is listed.

A dead letter is bounded in bytes as well, and is written whatever came
in: only what it holds is cut. Lengths are of the form written -- JSON
with every non-ASCII character escaped, so a character is a byte. A
value of `data` longer than **2048** is not kept, its key is dropped by
name; an `event` longer than **128** bytes is not kept, `event` is
dropped by name; `_dlq_dropped` lists the first **20** names in sorted
order, each at most **128** long as JSON, and `_dlq_dropped_more` counts
the names it does not list. `_dlq_error` is at most 2000 characters --
up to 8000 bytes in UTF-8. Every value a valid event sends fits, but a
`reminder_cancel` `types` list of many keys. A dead letter is then
about 25 KB at most, about 250 MB at the default `DLQ_MAXLEN`.

| field | what it holds |
|---|---|
| `event` | the entry's event name as sent; absent when the entry had none, or it was not UTF-8, or longer than 128 bytes |
| `_dlq_data` | a JSON object of the keys of `data` the event's own field set names (section 7): for `notification_request` the frame and the envelope without `llm_audio` -- the signed link to a person's voice (section 11); for the other events their fields; a key whose value is longer than 2048 as JSON is left out. Absent for an unknown event, and for a `data` that is not a UTF-8 JSON object |
| `_dlq_data_fingerprint` | the SHA-256 of the `data` bytes as sent -- the digest a rejection at intake records; absent without `data` |
| `_dlq_data_bytes` | the length of those bytes; absent without `data` |
| `_dlq_dropped` | a JSON list of the names of everything not kept: a field of the entry by its name, a key of `data` as `data.<key>`, the whole of `data` as `data`; sorted, the first 20 of those at most 128 long as JSON; `[]` when nothing was |
| `_dlq_dropped_more` | how many names of what was not kept the list leaves out -- past the first 20, or longer than 128; `0` when it lists them all |
| `_dlq_error` | why: comms' own refusal (it names fields and the address, never the letter), or the class and the place of the failure -- never an exception's text |
| `_dlq_attempts` | the attempts made; `0` -- refused as it was read |
| `_dlq_source_entry_id` | the entry's id in the stream it came from |
