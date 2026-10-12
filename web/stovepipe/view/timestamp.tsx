"use client";

const formatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium",
  timeStyle: "long",
  timeZone: "UTC",
});

/** Displays Unix milliseconds in UTC; zero or invalid values are unknown. */
export function Timestamp({ value }: { value: string | number }) {
  const date = new Date(Number(value));
  if (Number(value) <= 0 || Number.isNaN(date.getTime()))
    return <span>Unknown</span>;
  return (
    <time dateTime={date.toISOString()}>
      {formatter.format(date)}
    </time>
  );
}
