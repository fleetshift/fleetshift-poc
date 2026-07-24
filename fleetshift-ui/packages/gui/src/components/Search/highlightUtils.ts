// Fuzzy text matching and highlight mark generation.
// Adapted from insights-chrome's levenshtein-search implementation.

export interface Match {
  start: number;
  end: number;
  dist: number;
}

function* searchExact(
  needle: string,
  haystack: string,
  startIndex = 0,
  endIndex: number | null = null,
) {
  const needleLen = needle.length;
  if (needleLen === 0) return;
  if (endIndex === null) endIndex = haystack.length;

  let index: number;
  while ((index = haystack.indexOf(needle, startIndex)) > -1) {
    if (index + needle.length > endIndex!) break;
    yield index;
    startIndex = index + 1;
  }
}

function reverse(s: string) {
  return s.split("").reverse().join("");
}

function makeChar2needleIdx(needle: string, maxDist: number) {
  const res: Record<string, number> = {};
  for (let i = Math.min(needle.length - 1, maxDist); i >= 0; i--) {
    res[needle[i]] = i;
  }
  return res;
}

function _expand(
  needle: string,
  haystack: string,
  maxDist: number,
): [number | null, number | null] {
  let firstDiff: number;
  for (
    firstDiff = 0;
    firstDiff < Math.min(needle.length, haystack.length);
    firstDiff++
  ) {
    if (needle.charCodeAt(firstDiff) !== haystack.charCodeAt(firstDiff)) break;
  }
  if (firstDiff) {
    needle = needle.slice(firstDiff);
    haystack = haystack.slice(firstDiff);
  }

  if (!needle) return [0, firstDiff];
  if (!haystack)
    return needle.length <= maxDist ? [needle.length, firstDiff] : [null, null];
  if (maxDist === 0) return [null, null];

  let scores = new Array(needle.length + 1);
  for (let i = 0; i <= maxDist; i++) scores[i] = i;
  let newScores = new Array(needle.length + 1);

  let minScore: number | null = null;
  let minScoreIdx: number | null = null;
  let maxGoodScore = maxDist;
  let firstGoodScoreIdx: number | null = 0;
  let lastGoodScoreIdx = needle.length - 1;

  for (let haystackIdx = 0; haystackIdx < haystack.length; haystackIdx++) {
    const char = haystack.charCodeAt(haystackIdx);
    const needleIdxStart = Math.max(0, (firstGoodScoreIdx ?? 0) - 1);
    const needleIdxLimit = Math.min(
      haystackIdx + maxDist,
      needle.length - 1,
      lastGoodScoreIdx,
    );

    newScores[0] = scores[0] + 1;
    firstGoodScoreIdx = newScores[0] <= maxGoodScore ? 0 : null;
    lastGoodScoreIdx = newScores[0] <= maxGoodScore ? 0 : -1;

    let needleIdx: number;
    for (needleIdx = needleIdxStart; needleIdx < needleIdxLimit; needleIdx++) {
      const score = (newScores[needleIdx + 1] = Math.min(
        scores[needleIdx] + +(char !== needle.charCodeAt(needleIdx)),
        scores[needleIdx + 1] + 1,
        newScores[needleIdx] + 1,
      ));
      if (score <= maxGoodScore) {
        if (firstGoodScoreIdx === null) firstGoodScoreIdx = needleIdx + 1;
        lastGoodScoreIdx = Math.max(
          lastGoodScoreIdx,
          needleIdx + 1 + (maxGoodScore - score),
        );
      }
    }

    const lastScore = (newScores[needleIdx + 1] = Math.min(
      scores[needleIdx] + +(char !== needle.charCodeAt(needleIdx)),
      newScores[needleIdx] + 1,
    ));
    if (lastScore <= maxGoodScore) {
      if (firstGoodScoreIdx === null) firstGoodScoreIdx = needleIdx + 1;
      lastGoodScoreIdx = needleIdx + 1;
    }

    if (
      needleIdx === needle.length - 1 &&
      (minScore === null || lastScore <= minScore)
    ) {
      minScore = lastScore;
      minScoreIdx = haystackIdx;
      if (minScore < maxGoodScore) maxGoodScore = minScore;
    }

    [scores, newScores] = [newScores, scores];
    if (firstGoodScoreIdx === null) break;
  }

  if (minScore !== null && minScore <= maxDist) {
    return [minScore, (minScoreIdx ?? 0) + 1 + firstDiff];
  }
  return [null, null];
}

function* fuzzySearchNgrams(needle: string, haystack: string, maxDist: number) {
  const ngramLen = Math.floor(needle.length / (maxDist + 1));
  const needleLen = needle.length;
  const haystackLen = haystack.length;

  for (
    let ngramStartIdx = 0;
    ngramStartIdx <= needle.length - ngramLen;
    ngramStartIdx += ngramLen
  ) {
    const ngram = needle.slice(ngramStartIdx, ngramStartIdx + ngramLen);
    const ngramEnd = ngramStartIdx + ngramLen;
    const needleBeforeReversed = reverse(needle.slice(0, ngramStartIdx));
    const needleAfter = needle.slice(ngramEnd);
    const startIdx = Math.max(0, ngramStartIdx - maxDist);
    const endIdx = Math.min(
      haystackLen,
      haystackLen - needleLen + ngramEnd + maxDist,
    );

    for (const haystackMatchIdx of searchExact(
      ngram,
      haystack,
      startIdx,
      endIdx,
    )) {
      const [distRight, rightExpandSize] = _expand(
        needleAfter,
        haystack.slice(
          haystackMatchIdx + ngramLen,
          haystackMatchIdx - ngramStartIdx + needleLen + maxDist,
        ),
        maxDist,
      );
      if (distRight === null) continue;

      const [distLeft, leftExpandSize] = _expand(
        needleBeforeReversed,
        reverse(
          haystack.slice(
            Math.max(
              0,
              haystackMatchIdx - ngramStartIdx - (maxDist - distRight),
            ),
            haystackMatchIdx,
          ),
        ),
        maxDist - distRight,
      );
      if (distLeft === null) continue;

      yield {
        start: haystackMatchIdx - (leftExpandSize ?? 0),
        end: haystackMatchIdx + ngramLen + (rightExpandSize ?? 0),
        dist: distLeft + distRight,
      };
    }
  }
}

interface BoundedMetadata {
  startIdx: number;
  needleIdx: number;
  dist: number;
}

function* fuzzySearchCandidates(
  needle: string,
  haystack: string,
  maxDist: number,
) {
  const needleLen = needle.length;
  const haystackLen = haystack.length;
  if (needleLen > haystackLen + maxDist) return;
  const char2needleIdx = makeChar2needleIdx(needle, maxDist);

  let prevCandidates: BoundedMetadata[] = [];
  let candidates: BoundedMetadata[] = [];

  for (let i = 0; i < haystackLen; i++) {
    const haystackChar = haystack[i];
    prevCandidates = candidates;
    candidates = [];

    const needleIdx = char2needleIdx[haystackChar];
    if (needleIdx !== undefined) {
      if (needleIdx + 1 === needleLen) {
        yield { start: i, end: i + 1, dist: needleIdx };
      } else {
        candidates.push({
          startIdx: i,
          needleIdx: needleIdx + 1,
          dist: needleIdx,
        });
      }
    }

    for (const candidate of prevCandidates) {
      if (candidate.needleIdx && needle[candidate.needleIdx] === haystackChar) {
        if (candidate.needleIdx + 1 === needleLen) {
          yield { start: candidate.startIdx, end: i + 1, dist: candidate.dist };
        } else {
          candidates.push({
            startIdx: candidate.startIdx,
            needleIdx: candidate.needleIdx + 1,
            dist: candidate.dist,
          });
        }
      } else {
        if (candidate.dist === maxDist) continue;

        candidates.push({
          startIdx: candidate.startIdx,
          needleIdx: candidate.needleIdx,
          dist: candidate.dist + 1,
        });

        for (
          let nSkipped = 1;
          nSkipped <= maxDist - candidate.dist;
          nSkipped++
        ) {
          if (candidate.needleIdx + nSkipped === needleLen) {
            yield {
              start: candidate.startIdx,
              end: i + 1,
              dist: candidate.dist + nSkipped,
            };
            break;
          } else if (
            candidate.needleIdx &&
            needle[candidate.needleIdx + nSkipped] === haystackChar
          ) {
            if (candidate.needleIdx + nSkipped + 1 === needleLen) {
              yield {
                start: candidate.startIdx,
                end: i + 1,
                dist: candidate.dist + nSkipped,
              };
            } else {
              candidates.push({
                startIdx: candidate.startIdx,
                needleIdx: candidate.needleIdx + 1 + nSkipped,
                dist: candidate.dist + nSkipped,
              });
            }
            break;
          }
        }

        if (i + 1 < haystackLen && candidate.needleIdx + 1 < needleLen) {
          candidates.push({
            startIdx: candidate.startIdx,
            needleIdx: candidate.needleIdx + 1,
            dist: candidate.dist + 1,
          });
        }
      }
    }
  }

  for (const candidate of candidates) {
    const dist =
      (candidate.dist ?? 0) + needle.length - (candidate.needleIdx ?? 0);
    if (dist <= maxDist) {
      yield { start: candidate.startIdx, end: haystackLen, dist };
    }
  }
}

export function* fuzzySearch(
  needle: string,
  haystack: string,
  maxDist: number,
) {
  if (needle.length > haystack.length + maxDist) return;
  const ngramLen = Math.floor(needle.length / (maxDist + 1));

  if (maxDist === 0) {
    for (const index of searchExact(needle, haystack)) {
      yield { start: index, end: index + needle.length, dist: 0 };
    }
  } else if (ngramLen >= 10) {
    yield* fuzzySearchNgrams(needle, haystack, maxDist);
  } else {
    yield* fuzzySearchCandidates(needle, haystack, maxDist);
  }
}

export function minimumDistanceMatches(matches: Match[]): Match[] {
  let minDist: number | null = null;
  let out: Match[] = [];
  for (const match of matches) {
    if (minDist === null || match.dist < minDist) {
      minDist = match.dist;
      out = [match];
    } else if (match.dist === minDist) {
      out.push(match);
    }
  }
  return out;
}

function joinMatchPositions(marks: Match[]) {
  const cpy = [...marks];
  cpy.sort((a, b) => a.start - b.start);

  return cpy.reduce<{ start: number; end: number }[]>((acc, { start, end }) => {
    const bounded = acc.findIndex(
      (o) =>
        (o.start >= start && o.start <= end) ||
        (start >= o.start && start <= o.end),
    );
    if (bounded >= 0) {
      acc[bounded] = {
        start: Math.min(start, acc[bounded].start),
        end: Math.max(end, acc[bounded].end),
      };
    } else {
      acc.push({ start, end });
    }
    return acc;
  }, []);
}

function escapeHtml(s: string) {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function applyMarks(text: string, marks: { start: number; end: number }[]) {
  const cpy = [...marks];
  cpy.sort((a, b) => a.start - b.start);

  let out = "";
  let prevEnd = 0;
  for (const mark of cpy) {
    out += escapeHtml(text.substring(prevEnd, mark.start));
    out += `<mark>${escapeHtml(text.substring(mark.start, mark.end))}</mark>`;
    prevEnd = mark.end;
  }
  out += escapeHtml(text.substring(prevEnd));
  return out;
}

const LOWERCASE_A = "a".charCodeAt(0);
const UPPERCASE_A = "A".charCodeAt(0);
const UPPERCASE_Z = "Z".charCodeAt(0);

function asciiLowercase(value: string) {
  const out: number[] = [];
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    out.push(
      c >= UPPERCASE_A && c <= UPPERCASE_Z ? c - UPPERCASE_A + LOWERCASE_A : c,
    );
  }
  return String.fromCharCode(...out);
}

const HIGHLIGHT_CACHE_MAX = 500;
const highlightCache = new Map<string, string>();

export function highlightText(term: string, text: string): string {
  const key = `${term}\0${text}`;
  const cached = highlightCache.get(key);
  if (cached) return cached;

  const merged = joinMatchPositions(
    minimumDistanceMatches([
      ...fuzzySearch(asciiLowercase(term), asciiLowercase(text), 2),
    ]),
  );
  const result = applyMarks(text, merged);
  if (highlightCache.size >= HIGHLIGHT_CACHE_MAX) {
    const oldest = highlightCache.keys().next().value;
    if (oldest !== undefined) highlightCache.delete(oldest);
  }
  highlightCache.set(key, result);
  return result;
}
