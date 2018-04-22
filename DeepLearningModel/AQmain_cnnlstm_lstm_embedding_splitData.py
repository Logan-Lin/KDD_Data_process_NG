import sys
from utils import *

sys.path.append('../')
from DeepLearningModel.TestModel import *
from DeepLearningModel.TrainModel import *
import warnings
import argparse
import time
import numpy as np
import h5py
import os
from copy import copy
import csv


def float_m(value):
    if value is None or len(value) == 0:
        raise (ValueError("Data loss here"))
        # return None
    return float(value)


# Load all needed data into dicts and lists.
# Load aq station location dict
aq_location = dict()
with open("data/Beijing_AirQuality_Stations_cn.csv") as read_file:
    reader = csv.reader(read_file, delimiter='\t')
    for row in reader:
        try:
            aq_location[row[0]] = list(map(float_m, row[1:]))
        except ValueError:
            pass
# Load grid location dict
grid_location = dict()
with open("data/beijing_grid_location.csv") as read_file:
    reader = csv.reader(read_file, delimiter=',')
    for row in reader:
        try:
            grid_location[row[0]] = list(map(float_m, row[1:3]))
        except ValueError:
            pass
warnings.filterwarnings("ignore")
# 读出超参
# load训练集和测试集
# 这否归一化
# 训练模型，保存结果
# 是否反归一化
# 输出test 和 train上的预测结果
# AQNet_cnnlstm_lstm_embedding_realvalue
# AQNet_cnnlstm_lstm_embedding_normalvalue
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # real_data or normal_data
    parser.add_argument('-r', '--real_data', type=int,
                        default=0)  # if r=1, AQNet_cnnlstm_lstm_embedding_realvalue; else r=0,
    # AQNet_cnnlstm_lstm_embedding_normalvalue
    # len_history
    parser.add_argument('-hl', '--len_history', type=int, default=24)
    # meo_size
    parser.add_argument('-ms', '--meo_size', type=int, default=7)
    # nb_meo_lstm_encode
    parser.add_argument('-mle', '--meo_lstm_en', type=int, default=1)
    # nb_meo_lstm_decode
    parser.add_argument('-mld', '--meo_lstm_de', type=int, default=1)
    # nb_aq_lstm_encode
    parser.add_argument('-ale', '--aq_lstm_en', type=int, default=2)
    # nb_aq_lstm_decode
    parser.add_argument('-ald', '--aq_lstm_de', type=int, default=2)
    # len_test
    parser.add_argument('-test', '--test_start_date', type=int, default=1522512000)  # 1522512000 : 2018/4/1(BJ)
    # nb_start_epoch 训练开始的轮次等于(nb_start_epoch+1)
    # nb_start_epoch = -1 means test fixed epoch
    parser.add_argument('-start', '--nb_start_epoch', type=int, default=0)
    # nb_end_epoch 结束的训练轮次
    parser.add_argument('-end', '--nb_end_epoch', type=int, default=5)
    args = parser.parse_args()
    isRealData = args.real_data
    len_history = args.len_history
    meo_size = args.meo_size
    nb_meo_lstm_encode = args.meo_lstm_en
    nb_meo_lstm_decode = args.meo_lstm_de
    nb_aq_lstm_encode = args.aq_lstm_en
    nb_aq_lstm_decode = args.aq_lstm_de
    test_start_date = args.test_start_date
    nb_start_epoch = args.nb_start_epoch
    nb_end_epoch = args.nb_end_epoch

    # load DataSet
    all_X_meo = []
    all_X_aq = []
    all_Y = []
    all_timestampes_Y = []
    all_aq_id = []
    for aq_name in aq_location.keys():
        h5_file = h5py.File("data/h5/" + aq_name + ".h5", "r")
        grid_data_value = h5_file["grid"].value
        all_X_meo.append(grid_data_value)
        all_X_aq.append(h5_file["history"].value)
        all_Y.append(h5_file["predict"].value)
        all_timestampes_Y.append(h5_file["timestep"].value)
        all_aq_id.append(np.array([list(aq_location.keys()).index(aq_name) + 1] * np.shape(grid_data_value)[0]))
        h5_file.close()
    all_X_meo = np.moveaxis(np.vstack(copy(all_X_meo)), 4, 2)
    all_X_aq = np.vstack(copy(all_X_aq))
    all_Y = np.vstack(copy(all_Y))
    all_timestampes_Y = np.concatenate(copy(all_timestampes_Y), axis=0)
    split_index = splitTrainandTest(all_timestampes_Y, test_start_date)
    print("split_index:")

    all_X_external = None
    # isRealData = 0, means need normalization
    if isRealData == 0:
        all_X_meo, all_X_aq, meo_max, meo_min, aq_max, aq_min = transform_X(all_X_meo, all_X_aq)
        Y_aq_min = []
        Y_aq_max = []
        Y_aq_min.append(aq_min[0])
        Y_aq_max.append(aq_max[0])
        Y_aq_min.append(aq_min[1])
        Y_aq_max.append(aq_max[1])
        Y_aq_min.append(aq_min[4])
        Y_aq_max.append(aq_max[4])
        all_Y = transform_Y(all_Y, Y_aq_max, Y_aq_min)
    if all_X_external is None:
        external_dim = 0
        Train_X_meo, Train_X_aq, Y_train = all_X_meo[:split_index], all_X_aq[:split_index], all_Y[:split_index]
        Test_X_meo, Test_X_aq, Y_test = all_X_meo[split_index:], all_X_aq[split_index:], all_Y[split_index:]
        timestamps_train, timestamps_test = all_timestampes_Y[:split_index], all_timestampes_Y[split_index:]
        aq_id_train, aq_id_test = all_aq_id[:split_index], all_timestampes_Y[split_index:]
    else:
        external_dim = all_X_external.shape[1]
        Train_X_meo, Train_X_aq, Train_X_ext, Y_train = \
            all_X_meo[:split_index], all_X_aq[:split_index], all_X_external[:split_index], all_Y[:split_index]
        Test_X_meo, Test_X_aq, Test_X_ext, Y_test = \
            all_X_meo[split_index:], all_X_aq[split_index:], all_X_external[split_index:], all_Y[split_index:]
        timestamps_train, timestamps_test = all_timestampes_Y[:split_index], all_timestampes_Y[split_index:]
        aq_id_train, aq_id_test = all_aq_id[:split_index], all_timestampes_Y[split_index:]
    X_train = []
    X_test = []
    X_train.append(Train_X_meo)
    X_train.append(Train_X_aq)
    if external_dim > 0:
        X_train.append(Train_X_ext)
    X_test.append(Test_X_meo)
    X_test.append(Test_X_aq)
    if external_dim > 0:
        X_test.append(Test_X_ext)
    print("---------------- X train, Y train -----------------")
    for _X in X_train:
        print(_X.shape, end=',')
    print('\n', Y_train.shape)
    print()
    print("---------------- X test, Y test -----------------")
    for _X in X_test:
        print(_X.shape, end=',')
    print('\n', Y_test.shape)
    print()
    # strat train model #
    train_root_path = 'data/Model_Results'
    train_model_path = os.path.join(train_root_path)
    if not os.path.exists(train_model_path):
        os.makedirs(train_model_path)
        print("mkdir train_model_path:", train_model_path)
    lr = 0.0002
    # lr = 0.00002
    # lr = 0.000002
    batch_size = 32
    if nb_start_epoch >= 0:
        # training
        print("<" * 5, "train on train data", ">" * 5)
        start = time.time()
        train = TrainModel(train_model_path, lr, batch_size, nb_end_epoch, nb_start_epoch)
        if isRealData == 0:
            train.train_AQNet_cnnlstm_lstm_embedding_normalvalue(X_train, Y_train, len_history, meo_size,
                                                                 nb_meo_lstm_encode, nb_meo_lstm_decode,
                                                                 nb_aq_lstm_encode,
                                                                 nb_aq_lstm_decode, external_dim)
        else:
            train.train_AQNet_cnnlstm_lstm_embedding_realvalue(X_train, Y_train, len_history, meo_size,
                                                               nb_meo_lstm_encode,
                                                               nb_meo_lstm_decode, nb_aq_lstm_encode, nb_aq_lstm_decode,
                                                               external_dim)
        elapsed = (time.time() - start)
        print('train on train data:', elapsed, 's')
        # testing
        test_model_path = train_model_path
        # evaluate model on train data
        print('\n', "<" * 5, "evaluate on train data", ">" * 5)
        start = time.time()
        test = TestModel(test_model_path, batch_size, nb_start_epoch, Y_aq_max, Y_aq_min)
        if isRealData == 0:
            test.test_AQNet_cnnlstm_lstm_embedding_normalvalue(X_train, Y_train, timestamps_train, len_history,
                                                               meo_size,
                                                               nb_meo_lstm_encode, nb_meo_lstm_decode,
                                                               nb_aq_lstm_encode,
                                                               nb_aq_lstm_decode, external_dim)
        else:
            test.test_AQNet_cnnlstm_lstm_embedding_realvalue(X_train, Y_train, timestamps_train, len_history, meo_size,
                                                             nb_meo_lstm_encode, nb_meo_lstm_decode, nb_aq_lstm_encode,
                                                             nb_aq_lstm_decode, external_dim)
        elapsed = (time.time() - start)
        print('evaluate on train data:', elapsed, 's')
        # evaluate model on test data
        print('\n', "<" * 5, "evaluate on test data", ">" * 5)
        start = time.time()
        test = TestModel(test_model_path, batch_size, nb_start_epoch, Y_aq_max, Y_aq_min)
        if isRealData == 0:
            test.test_AQNet_cnnlstm_lstm_embedding_normalvalue(X_test, Y_test, timestamps_test, len_history, meo_size,
                                                               nb_meo_lstm_encode, nb_meo_lstm_decode,
                                                               nb_aq_lstm_encode,
                                                               nb_aq_lstm_decode, external_dim)
        else:
            test.test_AQNet_cnnlstm_lstm_embedding_realvalue(X_test, Y_test, timestamps_test, len_history, meo_size,
                                                             nb_meo_lstm_encode, nb_meo_lstm_decode, nb_aq_lstm_encode,
                                                             nb_aq_lstm_decode, external_dim)
        elapsed = (time.time() - start)
        print('evaluate on test data:', elapsed, 's')
    else:
        # testing
        test_model_path = train_model_path
        # evaluate model on train data
        print('\n', "<" * 5, "evaluate on train data", ">" * 5)
        start = time.time()
        test = TestModel(test_model_path, batch_size, nb_start_epoch, Y_aq_max, Y_aq_min)
        if isRealData == 0:
            test.test_AQNet_cnnlstm_lstm_embedding_normalvalue_saveResultsWithTime(meo_max, meo_min, aq_max, aq_min,
                                                                                   X_train, Y_train, timestamps_train,
                                                                                   len_history, meo_size,
                                                                                   nb_meo_lstm_encode,
                                                                                   nb_meo_lstm_decode,
                                                                                   nb_aq_lstm_encode,
                                                                                   nb_aq_lstm_decode, external_dim,
                                                                                   fixed_epoch=nb_end_epoch)
        else:
            test.test_AQNet_cnnlstm_lstm_embedding_realvalue_saveResultsWithTime(meo_max, meo_min, aq_max, aq_min,
                                                                                 X_train, Y_train, timestamps_train,
                                                                                 len_history, meo_size,
                                                                                 nb_meo_lstm_encode, nb_meo_lstm_decode,
                                                                                 nb_aq_lstm_encode,
                                                                                 nb_aq_lstm_decode, external_dim,
                                                                                 fixed_epoch=nb_end_epoch)
        elapsed = (time.time() - start)
        print('evaluate on train data:', elapsed, 's')

        # evaluate model on test data
        print('\n', "<" * 5, "evaluate on test data", ">" * 5)
        start = time.time()
        test = TestModel(test_model_path, batch_size, nb_start_epoch, Y_aq_max, Y_aq_min)
        if isRealData == 0:
            test.test_AQNet_cnnlstm_lstm_embedding_normalvalue_saveResultsWithTime(meo_max, meo_min, aq_max, aq_min,
                                                                                   X_test, Y_test, timestamps_test,
                                                                                   len_history, meo_size,
                                                                                   nb_meo_lstm_encode,
                                                                                   nb_meo_lstm_decode,
                                                                                   nb_aq_lstm_encode,
                                                                                   nb_aq_lstm_decode, external_dim,
                                                                                   fixed_epoch=nb_end_epoch)
        else:
            test.test_AQNet_cnnlstm_lstm_embedding_realvalue_saveResultsWithTime(meo_max, meo_min, aq_max, aq_min,
                                                                                 X_test, Y_test, timestamps_test,
                                                                                 len_history, meo_size,
                                                                                 nb_meo_lstm_encode, nb_meo_lstm_decode,
                                                                                 nb_aq_lstm_encode,
                                                                                 nb_aq_lstm_decode, external_dim,
                                                                                 fixed_epoch=nb_end_epoch)

        elapsed = (time.time() - start)
        print('evaluate on test data:', elapsed, 's')
