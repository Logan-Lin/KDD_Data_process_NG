import numpy as np
import time
import datetime


def computeRMSE(y_predict, y_real):
    assert (y_predict.shape[0] == y_real.shape[0])
    mse = np.mean(np.square(y_predict - y_real))
    rmse = np.sqrt(mse)
    return mse, rmse


def computeSMAPE(y_predict, y_real):
    assert (y_predict.shape[0] == y_real.shape[0])
    smape = np.mean(2 * np.abs(y_predict - y_real) / (y_predict + y_real))
    return smape


def smape(actual, predicted):
    a = np.abs(actual - predicted)

    b = actual + predicted

    return 2 * np.mean(np.divide(a, b, out=np.zeros_like(a), where=b != 0, casting='unsafe'))


# y_predict:(#,predict_interval)
# y_real:(#,predict_interval)
# x: (#,len_history)
def saveResults(filename, y_predict, y_real, x):
    assert (y_predict.shape[0] == y_real.shape[0]) and (y_real.shape[0] == x.shape[0]) and (
                y_predict.shape[1] == y_real.shape[1])
    print("filename", filename)
    f = open(filename, 'w')
    f.truncate()
    nb_sample = x.shape[0]
    predict_interval = y_predict.shape[1]
    len_history = x.shape[1]
    # write title
    line = ''
    for i in range(1, (predict_interval + len_history + 1)):
        line = line + str(i) + ','
    line = line + '\n'
    f.write(line)
    # write data
    for i in range(nb_sample):
        line_real = ''
        line_predict = ''
        # write history
        for h in range(len_history):
            line_real = line_real + str(x[i, h]) + ','
            line_predict = line_predict + str(x[i, h]) + ','
        # write real
        for p in range(predict_interval):
            line_real = line_real + str(y_real[i, p]) + ','
        # write predict
        for p in range(predict_interval):
            line_predict = line_predict + str(y_predict[i, p]) + ','
        line_real = line_real + '\n'
        line_predict = line_predict + '\n'
        f.write(line_real)
        f.write(line_predict)
    f.close()


# timestamps:(#,)
# y_predict:(#,predict_interval)
# y_real:(#,predict_interval)
# x: (#,len_history)
def saveResultsWithTime(timestamps, filename, y_predict, y_real, x):
    assert (timestamps.shape[0] == y_real.shape[0]) and (y_predict.shape[0] == y_real.shape[0]) and (
                y_real.shape[0] == x.shape[0]) and (y_predict.shape[1] == y_real.shape[1])
    print("filename", filename)
    f = open(filename, 'w')
    f.truncate()
    nb_sample = x.shape[0]
    predict_interval = y_predict.shape[1]
    len_history = x.shape[1]
    # write title
    line = 'present_time,'
    for i in range(1, (predict_interval + len_history + 1)):
        line = line + str(i) + ','
    line = line + '\n'
    f.write(line)
    # write data
    for i in range(nb_sample):
        present_time = datetime.datetime.fromtimestamp(timestamps[i])
        present_time = time.strftime('%Y-%m-%d %H', present_time.timetuple())
        line_real = present_time + ','
        line_predict = present_time + ','
        # write history
        for h in range(len_history):
            line_real = line_real + str(x[i, h]) + ','
            line_predict = line_predict + str(x[i, h]) + ','
        # write real
        for p in range(predict_interval):
            line_real = line_real + str(y_real[i, p]) + ','
        # write predict
        for p in range(predict_interval):
            line_predict = line_predict + str(y_predict[i, p]) + ','
        line_real = line_real + '\n'
        line_predict = line_predict + '\n'
        f.write(line_real)
        f.write(line_predict)
    f.close()
