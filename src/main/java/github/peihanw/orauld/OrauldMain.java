package github.peihanw.orauld;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import github.peihanw.ut.AppTicker;
import github.peihanw.ut.PubMethod;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;

import static github.peihanw.ut.Stdout.*;

public class OrauldMain {

    /**
	 * 新增的公共入口方法，供外部应用调用
	 * @param args 命令行参数
	 * @return 退出码
	 * @throws Exception 如果执行过程中发生异常
	 */
	public static int run(String[] args) throws Exception {
		try {
			return _mainInternal(args);
		} catch (Exception e) {
			P(ERO, e, "encounter exception");
			return OrauldConst.EXIT_CODE_4_EXCEPTION;
		}
	}

	/**
	 * 内部执行逻辑，返回退出码而不是直接退出JVM
	 * @param args 命令行参数
	 * @return 退出码
	 * @throws Exception 如果执行过程中发生异常
	 */
	@SuppressWarnings("unchecked")
	private static int _mainInternal(String[] args) throws Exception {
		OrauldCmdline cmdline_ = OrauldCmdline.GetInstance();
		cmdline_.init(args);
		AppTicker ticker_ = new AppTicker();
		P(INF, "cmdline parsed and started");
		cmdline_.print();
        BlockingQueue<OrauldTuple>[] _UpQueues = (BlockingQueue<OrauldTuple>[]) new DisruptorBlockingQueue<?>[cmdline_._wrkNum];
        BlockingQueue<OrauldTuple>[] _DnQueues = (BlockingQueue<OrauldTuple>[]) new DisruptorBlockingQueue<?>[cmdline_._wrkNum];
        OrauldWrkRunnable[] _WrkRunnables = new OrauldWrkRunnable[cmdline_._wrkNum];
        Thread[] _WrkThreads = new Thread[cmdline_._wrkNum];
		for (int i = 0; i < cmdline_._wrkNum; ++i) {
			_UpQueues[i] = new DisruptorBlockingQueue<>(2000);
			_DnQueues[i] = new DisruptorBlockingQueue<>(2000);
		}

        OrauldDmpRunnable _DmpRunnable = new OrauldDmpRunnable(_UpQueues, _DnQueues);
        Thread _DmpThread = new Thread(_DmpRunnable, "DUMP");
		_DmpThread.start();

		for (int i = 0; i < cmdline_._wrkNum; ++i) {
			_WrkRunnables[i] = new OrauldWrkRunnable(_UpQueues[i], _DnQueues[i]);
			_WrkThreads[i] = new Thread(_WrkRunnables[i], "WRK" + i);
			_WrkThreads[i].start();
		}

		OrauldMgr mgr_ = new OrauldMgr(_UpQueues);
		int rc_ = mgr_.run();
		P(INF, "total %,d records fetched", mgr_._sqlCnt);

		for (int i = 0; i < cmdline_._wrkNum; ++i) {
			P(DBG, "try join thread %s", _WrkThreads[i].getName());
			_WrkThreads[i].join();
			P(DBG, "thread %s joined", _WrkThreads[i].getName());
		}

		P(DBG, "try join thread %s", _DmpThread.getName());
		_DmpThread.join();
		P(DBG, "thread %s joined", _DmpThread.getName());

		if (mgr_._sqlCnt == _DmpRunnable._dmpCnt) {
			P(INF, "balance ok, fetched eq dumped, %d %d", mgr_._sqlCnt, _DmpRunnable._dmpCnt);
		} else {
			P(ERO, "balance FAILED, fetched ne dumped, %d %d", mgr_._sqlCnt, _DmpRunnable._dmpCnt);
			if (rc_ == OrauldConst.EXIT_CODE_0_SUCCESS) {
				rc_ = OrauldConst.EXIT_CODE_3_IMBALANCE;
			}
		}

		mgr_.closeResource();
		ticker_.tickEnd(mgr_._sqlCnt);
		P(INF, "%s cnt term pfm %d %.3f %d", cmdline_._bcpFnm, ticker_._recNum, ticker_._term, ticker_._pfm);
		P(INF, "output file %s closed at %d rows", cmdline_._bcpFnm, ticker_._recNum);
		_printLogFile(cmdline_, ticker_);
		return rc_;
	}

	private static void _printLogFile(OrauldCmdline _cmdline, AppTicker ticker_) throws Exception {
		if (PubMethod.IsEmpty(_cmdline._logFile)) {
			return;
		}
		FileOutputStream fos_ = new FileOutputStream(_cmdline._logFile);
		OutputStreamWriter osw_ = new OutputStreamWriter(fos_, _cmdline._charset);
		PrintWriter pw_ = new PrintWriter(osw_);
		pw_.printf("output file %s closed at %d rows", _cmdline._bcpFnm, ticker_._recNum);
		pw_.flush();
		pw_.close();
	}

	// 保持原有的main方法不变
	public static void main(String[] args) throws Exception {
		System.exit(run(args));
	}
}
