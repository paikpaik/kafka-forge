const TOPIC_NAME_PATTERN = /^[a-z][a-z0-9-]*\.[a-z][a-z0-9-]*\.v[0-9]+$/;

export function assertValidTopicName(topic: string): void {
  if (!TOPIC_NAME_PATTERN.test(topic)) {
    throw new Error(
      `토픽 이름 "${topic}"이 네이밍 컨벤션(<domain>.<event>.v<N>, 소문자와 하이픈만 허용)을 따르지 않습니다.`,
    );
  }
}

export function createTopicName(domain: string, event: string, version: number): string {
  const topic = `${domain}.${event}.v${version}`;
  assertValidTopicName(topic);
  return topic;
}
