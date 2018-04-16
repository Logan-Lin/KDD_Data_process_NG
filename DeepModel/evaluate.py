import numpy as np

def computeRMSE(y_predict,y_real):
	assert(y_predict.shape[0] == y_real.shape[0])
	mse = np.mean(np.square(y_predict - y_real))
	rmse = np.sqrt(mse)
	return mse,rmse

def computeSMAPE(y_predict,y_real):
	assert(y_predict.shape[0] == y_real.shape[0])
	smape = np.mean(2*np.abs(y_predict - y_real)/(y_predict+y_real))
	return smape