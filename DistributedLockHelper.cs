using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Configuration;
using System.Globalization;

namespace XXX
{
    public interface IDistributedLockHelper
    {
        Task RunExclusiveAsync(string lockName, Func<Task> action);
    }

    public class DistributedLockHelper : IDistributedLockHelper
    {
        private static readonly ILogger<DistributedLockHelper> _logger = LogHelper.GetLogger<DistributedLockHelper>();
        private readonly IDatabase _database;
        private static readonly TimeSpan _keyExpiration = TimeSpan.Parse(ConfigurationManager.AppSettings["LockKeyExpirationInterval"] ?? "00:01:00", CultureInfo.InvariantCulture);
        private static readonly TimeSpan _keyExpirationRenew = TimeSpan.Parse(ConfigurationManager.AppSettings["LockKeyExpirationRenewInterval"] ?? "00:00:15", CultureInfo.InvariantCulture);
        private static readonly TimeSpan _acquisitionRetryPeriod = TimeSpan.Parse(ConfigurationManager.AppSettings["LockAcquireRetryPeriodInterval"] ?? "00:30:00", CultureInfo.InvariantCulture);
        private static readonly TimeSpan _acquisitionRetryDelay = TimeSpan.Parse(ConfigurationManager.AppSettings["LockAcquireRetryInterval"] ?? "00:00:03", CultureInfo.InvariantCulture);

        public DistributedLockHelper(IRedisManager redisManager)
        {
            _database = redisManager.RedisDatabase;
        }

        public async Task RunExclusiveAsync(string lockName, Func<Task> action)
        {
            var (redisKey, redisValue) = await AcquireLockAsync(lockName);

            CancellationTokenSource extendTokenSource = new CancellationTokenSource();
            var extendTask = ExtendKeyExpirationAsync(redisKey, redisValue, extendTokenSource.Token);

            try
            {
                await action();
            }
            finally
            {
                extendTokenSource.Cancel();
                await extendTask;
                await ReleaseLock(redisKey, redisValue);
            }
        }

        private async Task<(RedisKey, RedisValue)> AcquireLockAsync(string lockName)
        {
            _logger.LogInformation("AcquireLockAsync() - acquiring lock...");
            using (CancellationTokenSource acquisitionTokenSource = new CancellationTokenSource(_acquisitionRetryPeriod))
            {
                bool lockAcquired = false;
                RedisKey key = (RedisKey)lockName;
                RedisValue keyValue = (RedisValue)Guid.NewGuid().ToString();
                while (!lockAcquired)
                {
                    acquisitionTokenSource.Token.ThrowIfCancellationRequested();
                    lockAcquired = await _database.StringSetAsync(
                        key,
                        keyValue,
                        _keyExpiration,
                        When.NotExists);

                    if (lockAcquired)
                    {
                        _logger.LogInformation("AcquireLockAsync() - Lock acquired");
                        break;
                    }
                    await Task.Delay(_acquisitionRetryDelay, acquisitionTokenSource.Token);
                }
                return (key, keyValue);
            }
        }

        private async Task ExtendKeyExpirationAsync(RedisKey redisKey, RedisValue redisValue, CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var transaction = _database.CreateTransaction();
                    //Checking if a key has given value
                    var ownersipCondition = transaction.AddCondition(Condition.StringEqual(redisKey, redisValue));
                    // Set new expiration
                    transaction.KeyExpireAsync(redisKey, _keyExpiration);
                    var renewResult = await transaction.ExecuteAsync();
                    if (!renewResult)
                    {
                        if (!ownersipCondition.WasSatisfied)
                        {
                            _logger.LogError("ExtendKeyExpirationAsync() - The given value does not match with a key {key}", redisKey.ToString());
                            break;
                        }
                        else
                        {
                            _logger.LogError("ExtendKeyExpirationAsync() - Error while trying to extend expiration for lock {key}", redisKey.ToString());
                            break;
                        }
                    }
                    await Task.Delay(_keyExpirationRenew, token);
                }
            }
            catch (TaskCanceledException ex) when (ex.CancellationToken == token)
            {
                // This exception is expected when task is finished through token cancellation. Nothing to do.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ExtendKeyExpirationAsync() - Unexpected error occured while extending key expiration");
            }
        }

        private async Task ReleaseLock(RedisKey redisKey, RedisValue redisValue)
        {
            try
            {
                var transaction = _database.CreateTransaction();
                var ownersipCondition = transaction.AddCondition(Condition.StringEqual(redisKey, redisValue));
                transaction.KeyDeleteAsync(redisKey);
                var releaseResult = await transaction.ExecuteAsync();
                if (!releaseResult)
                {
                    if (!ownersipCondition.WasSatisfied)
                    {
                        _logger.LogError("ReleaseLock() - The given value does not match with a key {key}", redisKey.ToString());
                    }
                    else
                    {
                        _logger.LogError("ReleaseLock() - Error while trying to release lock {key}", redisKey.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ReleaseLock() - Unexpected error occured while releasing the lock");
            }
        }
    }
}
