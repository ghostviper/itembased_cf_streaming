用Python实现的Itembased CF算法，可以计算海量的item相似度，支持常用的相似度计算方法。根据业务不同item可以代表不同意思，
在电商业务中item可以代表商品，在搜索业务中item可以代表网页。该方法与Mahout中的CF相比，更节省内存，时间复杂度更低，2亿的item计算，只需要1.5g内存。稍后还会有更新版本出现，完全基于流式计算，对内存没有要求。
具体实现逻辑见：http://blog.csdn.net/zc02051126/article/details/47748617
