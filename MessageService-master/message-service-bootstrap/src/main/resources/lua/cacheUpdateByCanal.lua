
redis.call('del', KEYS[1]);

redis.call('setex', KEYS[1], 2626560, ARGV[1]);
