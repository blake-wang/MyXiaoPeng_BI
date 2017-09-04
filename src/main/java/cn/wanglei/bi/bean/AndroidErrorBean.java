package cn.wanglei.bi.bean;

import java.util.List;

/**
 * Created by Mr_yang on 2017/3/6.
 */

public class AndroidErrorBean {


    /**
     * data : [{"_id":1,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onPause]","time":1488614448119},{"_id":2,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onResume]","time":1488614491368},{"_id":3,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onPause]","time":1488615083538},{"_id":4,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onStop]","time":1488615085245},{"_id":5,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onDestroy]","time":1488615085279},{"_id":6,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onCreate]","time":1488615114178},{"_id":7,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onStart]","time":1488615114657},{"_id":8,"isThrowable":false,"msg":"--LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onResume]","time":1488615114686},{"_id":9,"isThrowable":false,"msg":"--EXCEPTION class: [com.example.commonlibtest.MainActivity]  exception: [java.lang.Exception: test exception\n\tat com.example.commonlibtest.MainActivity.lambda$-com_example_commonlibtest_MainActivity_lambda$4(MainActivity.java:79)\n\tat com.example.commonlibtest.-$Lambda$3.$m$0(Unknown Source)\n\tat com.example.commonlibtest.-$Lambda$3.call(Unknown Source)\n\tat rx.internal.operators.OperatorMap$MapSubscriber.onNext(OperatorMap.java:66)\n\tat rx.internal.util.ScalarSynchronousObservable$WeakSingleProducer.request(ScalarSynchronousObservable.java:268)\n\tat rx.Subscriber.setProducer(Subscriber.java:211)\n\tat rx.internal.operators.OperatorMap$MapSubscriber.setProducer(OperatorMap.java:99)\n\tat rx.internal.util.ScalarSynchronousObservable$1.call(ScalarSynchronousObservable.java:79)\n\tat rx.internal.util.ScalarSynchronousObservable$1.call(ScalarSynchronousObservable.java:75)\n\tat rx.internal.operators.OnSubscribeLift.call(OnSubscribeLift.java:50)\n\tat rx.internal.operators.OnSubscribeLift.call(OnSubscribeLift.java:30)\n\tat rx.internal.operators.OnSubscribeLift.call(OnSubscribeLift.java:50)\n\tat rx.internal.operators.OnSubscribeLift.call(OnSubscribeLift.java:30)\n\tat rx.Observable.unsafeSubscribe(Observable.java:8460)\n\tat rx.internal.operators.OperatorSubscribeOn$1.call(OperatorSubscribeOn.java:94)\n\tat rx.internal.schedulers.CachedThreadScheduler$EventLoopWorker$1.call(CachedThreadScheduler.java:222)\n\tat rx.internal.schedulers.ScheduledAction.run(ScheduledAction.java:55)\n\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:423)\n\tat java.util.concurrent.FutureTask.run(FutureTask.java:237)\n\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:154)\n\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:269)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1113)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:588)\n\tat java.lang.Thread.run(Thread.java:833)\n]","time":1488615123403}]
     * app_versioncode : 1
     * app_versionname : 1.0
     * channel_number :
     * device_id : A000004FA7523E
     * imei : A000004FA7523E
     * model : Che1-CL10
     * platform : 1
     * sdk_version : 23
     * signed_md_five : e74b6022f354f1ed0f1e73c81c6a6694
     */

    private String app_versioncode;
    private String app_versionname;
    private String channel_number;
    private String device_id;
    private String imei;
    private String model;
    private String platform;
    private String sdk_version;
    private String signed_md_five;
    private List<DataBean> data;

    public String getApp_versioncode() {
        return app_versioncode;
    }

    public void setApp_versioncode(String app_versioncode) {
        this.app_versioncode = app_versioncode;
    }

    public String getApp_versionname() {
        return app_versionname;
    }

    public void setApp_versionname(String app_versionname) {
        this.app_versionname = app_versionname;
    }

    public String getChannel_number() {
        return channel_number;
    }

    public void setChannel_number(String channel_number) {
        this.channel_number = channel_number;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getSdk_version() {
        return sdk_version;
    }

    public void setSdk_version(String sdk_version) {
        this.sdk_version = sdk_version;
    }

    public String getSigned_md_five() {
        return signed_md_five;
    }

    public void setSigned_md_five(String signed_md_five) {
        this.signed_md_five = signed_md_five;
    }

    public List<DataBean> getData() {
        return data;
    }

    public void setData(List<DataBean> data) {
        this.data = data;
    }

    public static class DataBean {
        /**
         * _id : 1
         * isThrowable : false
         * msg : --LIFECYCLE class: [com.example.commonlibtest.MainActivity]  method: [onPause]
         * time : 1488614448119
         */
        private String device_id;
        private int _id;
        private boolean isThrowable;
        private String msg;
        private long time;

        public String getDevice_id() {
            return device_id;
        }

        public void setDevice_id(String device_id) {
            this.device_id = device_id;
        }

        public int get_id() {
            return _id;
        }

        public void set_id(int _id) {
            this._id = _id;
        }

        public boolean isIsThrowable() {
            return isThrowable;
        }

        public void setIsThrowable(boolean isThrowable) {
            this.isThrowable = isThrowable;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

    }
}
