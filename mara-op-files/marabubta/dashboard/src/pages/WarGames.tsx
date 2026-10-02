// Marabunta - Licensed under the MIT License.
import { useState } from 'react';
import { issueChaosStrike, issueChaosRevive, DeathType } from '../services/chaosApi';
import { useNodes } from '../hooks/useSwarmData';

export function WarGames() {
    const { data: nodes } = useNodes();
    const [selectedRegion, setSelectedRegion] = useState<string>('US-East');
    const [selectedDeath, setSelectedDeath] = useState<string>('NetworkPartition');
    const [durationSecs, setDurationSecs] = useState<number>(60);
    const [loading, setLoading] = useState(false);
    const [resultMsg, setResultMsg] = useState<string | null>(null);

    const regions = Array.from(new Set((nodes || []).map(n => n.region).filter(Boolean)));

    const getDeathPayload = (): DeathType => {
        const dur = { secs: durationSecs, nanos: 0 };
        if (selectedDeath === 'FatalCrash') return 'FatalCrash';
        return { [selectedDeath]: { duration: dur } } as any;
    };

    const handleStrike = async () => {
        setLoading(true);
        setResultMsg(null);
        try {
            await issueChaosStrike([], selectedRegion, getDeathPayload());
            setResultMsg(`SUCCESS: Issued ${selectedDeath} strike to region ${selectedRegion}. The network partition simulation is now active.`);
        } catch (e: any) {
            setResultMsg(`ERROR: ${e.message}`);
        } finally {
            setLoading(false);
        }
    };

    const handleRevive = async () => {
        setLoading(true);
        setResultMsg(null);
        try {
            await issueChaosRevive([], selectedRegion);
            setResultMsg(`SUCCESS: Issued Revive command to region ${selectedRegion}. Nodes will begin accepting traffic and re-joining Kademlia.`);
        } catch (e: any) {
            setResultMsg(`ERROR: ${e.message}`);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="p-8 max-w-4xl mx-auto text-slate-200">
            <h1 className="text-3xl font-bold text-red-500 border-b-2 border-red-500 pb-2 mb-8">
                WAR GAMES: Chaos Engineering Console
            </h1>
            
            <div className="bg-slate-800 p-6 rounded-lg border border-slate-700 shadow-xl">
                <p className="mb-6 text-slate-400">
                    WARNING: These commands inject cryptographically-signed `ChaosStrike` packets into the live Kademlia network. Targeted nodes will physically alter their routing physics to simulate catastrophic hardware and network failures. This is designed to prove Byzantine Fault Tolerance and Aggregator resilience to investors.
                </p>

                <div className="mb-6">
                    <label className="block mb-2 font-bold text-slate-300">Target Geographic Region</label>
                    <select 
                        value={selectedRegion} 
                        onChange={e => setSelectedRegion(e.target.value)}
                        className="w-full p-2 bg-slate-900 text-white border border-slate-600 rounded"
                    >
                        {regions.map(r => <option key={r as string} value={r as string}>{r as string}</option>)}
                        {!regions.includes('US-East') && <option value="US-East">US-East</option>}
                        {!regions.includes('EU-West') && <option value="EU-West">EU-West</option>}
                    </select>
                </div>

                <div className="mb-6">
                    <label className="block mb-2 font-bold text-slate-300">Failure Mode (Death Type)</label>
                    <select 
                        value={selectedDeath} 
                        onChange={e => setSelectedDeath(e.target.value)}
                        className="w-full p-2 bg-slate-900 text-white border border-slate-600 rounded"
                    >
                        <option value="NetworkPartition">Network Partition (Drop all UDP/Kademlia traffic)</option>
                        <option value="ByzantineCorruption">Byzantine Corruption (Return mathematically invalid ZK-Proofs to Aggregator)</option>
                        <option value="ThermalPanic">Thermal Panic (Biological Load Shedding - Reject new chunks)</option>
                        <option value="JitterStorm">Jitter Storm (50% Packet Loss / Satellite Link Degradation)</option>
                        <option value="FatalCrash">Fatal Crash (Execute std::process::exit - PERMANENT)</option>
                    </select>
                </div>

                {selectedDeath !== 'FatalCrash' && (
                    <div className="mb-8">
                        <label className="block mb-2 font-bold text-slate-300">Simulation Duration (Seconds)</label>
                        <input 
                            type="number" 
                            value={durationSecs} 
                            onChange={e => setDurationSecs(Number(e.target.value))}
                            className="w-full p-2 bg-slate-900 text-white border border-slate-600 rounded"
                        />
                    </div>
                )}

                <div className="flex gap-4 mt-8">
                    <button 
                        onClick={handleStrike}
                        disabled={loading}
                        className="bg-red-600 hover:bg-red-700 text-white font-bold py-3 px-6 rounded shadow-lg disabled:opacity-50 transition-colors"
                    >
                        {loading ? 'TRANSMITTING STRIKE...' : 'INITIATE CHAOS STRIKE'}
                    </button>
                    
                    <button 
                        onClick={handleRevive}
                        disabled={loading}
                        className="bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-3 px-6 rounded shadow-lg disabled:opacity-50 transition-colors"
                    >
                        {loading ? 'TRANSMITTING REVIVE...' : 'REVIVE REGION'}
                    </button>
                </div>

                {resultMsg && (
                    <div className={`mt-6 p-4 rounded border ${resultMsg.startsWith('ERROR') ? 'bg-red-900 border-red-700 text-red-100' : 'bg-emerald-900 border-emerald-700 text-emerald-100'}`}>
                        {resultMsg}
                    </div>
                )}
            </div>
        </div>
    );
}
